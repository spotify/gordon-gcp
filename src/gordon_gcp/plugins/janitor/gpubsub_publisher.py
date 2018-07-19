# -*- coding: utf-8 -*-
#
# Copyright 2018 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Client module to publish any required DNS changes initiated from
:class:`.GDNSReconciler` to `Google Cloud Pub/Sub <https://cloud.
google.com/pubsub/docs/overview>`_. The consumer of these messages is
the `Gordon service <https://github.com/spotify/gordon>`_.

This client wraps around `google-cloud-pubsub <https://pypi.python.org
/pypi/google-cloud-pubsub>`_ using `grpc <https://github.com/googleapis/
googleapis/blob/master/google/pubsub/v1/pubsub.proto>`_ rather than
inheriting from :class:`.AIOConnection`.

.. attention::

    This publisher client is an internal module for the core janitor
    logic. No other use cases are expected.


To use:

.. code-block:: python

    import asyncio
    import gordon_gcp

    config = {
        'keyfile': '/path/to/keyfile.json',
        'project': 'a-dns-project',
        'topic': 'a-topic',
    }
    changes_channel = asyncio.Queue()

    publisher = gordon_gcp.get_gpubsub_publisher(
        config, changes_channel)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(publisher.start())
    finally:
        loop.close()
"""

import asyncio
import datetime
import functools
import json
import logging
import os

import zope.interface
from asyncio_extras import threads
from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub
from gordon_janitor import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth


__all__ = ('GPubsubPublisher',)


class GPubsubPublisherBuilder:
    """Build and configure a :class:`GPubsubPublisher` object.

    Args:
        config (dict): Google Cloud Pub/Sub-related configuration.
        changes_channel (asyncio.Queue): queue to publish message to
            make corrections to Cloud DNS.
    """
    def __init__(self, config, changes_channel, **kwargs):
        self.config = config
        self.changes_channel = changes_channel
        self.kwargs = kwargs

    def _validate_config(self):
        # req keys: keyfile, project, topic
        # TODO (lynn): keyfile won't be required once we support other
        #              auth methods
        if not self.config.get('keyfile'):
            msg = ('The path to a Service Account JSON keyfile is required to '
                   'authenticate for Google Cloud Pub/Sub.')
            logging.error(msg)
            raise exceptions.GCPConfigError(msg)
        if not self.config.get('project'):
            msg = 'The GCP project where Cloud Pub/Sub is located is required.'
            logging.error(msg)
            raise exceptions.GCPConfigError(msg)
        if not self.config.get('topic'):
            msg = ('A topic for the client to publish to in Cloud Pub/Sub is '
                   'required.')
            logging.error(msg)
            raise exceptions.GCPConfigError(msg)

        topic_prefix = f'projects/{self.config["project"]}/topics/'
        if not self.config.get('topic').startswith(topic_prefix):
            self.config['topic'] = f'{topic_prefix}{self.config["topic"]}'

    def _init_auth(self):
        # a publisher client can't be made with credentials if a channel is
        # already made/provided, which is what happens when the emulator is
        # running ಠ_ಠ
        # See (https://github.com/GoogleCloudPlatform/
        #      google-cloud-python/pull/4839)
        auth_client = None
        if not os.environ.get('PUBSUB_EMULATOR_HOST'):
            scopes = self.config.get('scopes')
            # creating a dummy `session` as the pubsub.PublisherClient never
            # uses it but without it aiohttp will complain about an unclosed
            # client session that would otherwise be made by default
            auth_client = auth.GAuthClient(
                keyfile=self.config['keyfile'], scopes=scopes, session='noop')
        return auth_client

    def _init_client(self, auth_client):
        # Silly emulator constraints
        creds = getattr(auth_client, 'creds', None)
        _client = pubsub.PublisherClient(credentials=creds)

        topic = self.config['topic']
        try:
            _client.create_topic(topic)
        except google_exceptions.AlreadyExists:
            # already created
            pass
        except Exception as e:
            msg = f'Error trying to create topic "{topic}": {e}'
            logging.error(msg, exc_info=e)
            raise exceptions.GCPGordonJanitorError(msg)

        return _client

    def build_publisher(self):
        self._validate_config()
        auth_client = self._init_auth()
        pubsub_client = self._init_client(auth_client)
        return GPubsubPublisher(
            self.config, pubsub_client, self.changes_channel, **self.kwargs)


@zope.interface.implementer(interfaces.IPublisher)
class GPubsubPublisher:
    """Client to publish change messages to Google Pub/Sub.

    Args:
        config (dict): Google Cloud Pub/Sub-related configuration, ex.
            'projects/test-example/topics/a-topic'.
        publisher (google.cloud.pubsub_v1.publisher.client.Client):
            client to interface with Google Pub/Sub API.
        changes_channel (asyncio.Queue): queue to publish message to
            make corrections to Cloud DNS.
    """

    def __init__(self, config, publisher, changes_channel=None, **kw):
        self.topic = config['topic']
        self.publisher = publisher
        self.changes_channel = changes_channel
        self.cleanup_timeout = config.get('cleanup_timeout', 60)
        self.sleep_for = 0.5  # half a second
        self._messages = set()

    async def cleanup(self):
        """Clean up outstanding tasks and emit final logs + metrics.

        This method collects all tasks that this particular class
        initiated, and will cancel them if they don't complete within
        the configured timeout period.
        """
        tasks_to_clear = [
            t for t in self._messages if not t.done()
        ]

        iterations = self.cleanup_timeout / self.sleep_for
        while iterations:
            tasks_to_clear = [t for t in tasks_to_clear if not t.done()]
            if not tasks_to_clear:
                break

            await asyncio.sleep(self.sleep_for)
            iterations -= 1

        # give up on waiting for tasks to complete
        if tasks_to_clear:
            msg = (f'The following tasks did not complete in time and are '
                   f'being cancelled: {tasks_to_clear}')
            logging.warning(msg)
            for task in tasks_to_clear:
                task.cancel()

        # TODO (lynn): add metrics.flush call here once aioshumway is released
        msg = ('Finished sending reconciliation messages to Google Pub/Sub.')
        logging.info(msg)

    def _message_publish_callback(self, message, future):
        action = message['action']
        name = message['resourceRecords']['name']
        msg_id = future.result()
        self._messages.remove(future)
        logging.debug(f'Message published for {action}:{name} as {msg_id},'
                      f'currently tracking {len(self._messages)} messages.')

    @threads.threadpool
    def publish(self, message):
        """Publish received change message to Google Pub/Sub.

        Args:
            message (dict): change message received from the
                :obj:`changes_channel` to emit.
        """
        message['timestamp'] = datetime.datetime.utcnow().isoformat()
        bytes_message = bytes(json.dumps(message), encoding='utf-8')
        future = self.publisher.publish(self.topic, bytes_message)

        # collect to make sure everything's cleaned up once done
        self._messages.add(future)
        # TODO (lynn): add metrics.incr/emit call here once aioshumway
        #              is released
        future.add_done_callback(
            functools.partial(self._message_publish_callback, message))

    async def run(self):
        """Start consuming from :obj:`changes_channel`.

        Once ``None`` is received from the channel, finish processing
        records and clean up any outstanding tasks.
        """
        while True:
            change_message = await self.changes_channel.get()
            if change_message is None:
                break
            # TODO (lynn): emit metric of message received once
            #              aioshumway is released
            try:
                await self.publish(change_message)
            except Exception as e:  # todo
                logging.error('Exception while trying to publish message to '
                              f' pubsub: {e}')

        await self.cleanup()
