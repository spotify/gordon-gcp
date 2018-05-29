# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 Spotify AB
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
Client module to consume Google Cloud Pub/Sub messages and create an
event message to be passed around the ``gordon`` core system and its
plugins. Once an event message is created, it will be placed into the
``success_channel`` to be further handled by the ``gordon`` core system.
While being processed by the other plugin(s), the consumer will
continuously extend the message's ``ack`` deadline. When an event
message is done (either successfully published or met with errors) the
consumer will then ``ack`` the message in Pub/Sub to signify that the
work is complete.

.. attention::
    The event consumer client is an internal module for the core gordon
    logic. No other use cases are expected.

"""

import asyncio
import concurrent.futures
import datetime
import functools
import json
import logging
import os
import threading

import zope.interface
from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.plugins import _utils
from gordon_gcp.schema import parse
from gordon_gcp.schema import validate


__all__ = ('GEventMessage', 'GPSEventConsumer')


@zope.interface.implementer(interfaces.IEventMessage)
class GEventMessage:
    """Represent a unit of work related to Google Cloud.

    Args:
        pubsub_msg (google.cloud.pubsub_v1.subscriber.message.Message):
            Google Pub/Sub message.
        data (dict): data to be enriched (if record info is missing) via
            the IEnricher plugin, or to be published via the IPublisher
            plugin.
        phase (str): (optional) starting phase of the message. Defaults
            to ``None``.
    """
    def __init__(self, pubsub_msg, data, phase=None):
        self.msg_id = pubsub_msg.message_id
        self._pubsub_msg = pubsub_msg
        self.data = data
        self.phase = phase
        self.history_log = []

    def append_to_history(self, message, plugin):
        """Add to log history of the message.

        Args:
            message (str): log entry message
            plugin (str): plugin that created the log entry message.
        """
        datefmt = '%Y-%m-%dT%H:%M:%S.%fZ%z'  # 2018-03-27T13:29:12.623222Z
        now = datetime.datetime.utcnow()
        log_item = {
            'timestamp': now.strftime(datefmt),
            'plugin': plugin,
            'message': message
        }
        self.history_log.append(log_item)


class GPSEventConsumerBuilder:
    """Build and configure a :class:`GPSEventConsumer` object.

    Args:
        config (dict): Google Cloud Pub/Sub-related configuration.
        success_channel (asyncio.Queue): queue to place a successfully
            consumed message to be further handled by the ``gordon``
            core system.
        error_channel (asyncio.Queue): queue to place a message met
            with errors to be further handled by the ``gordon`` core
            system.
        kwargs (dict): Additional keyword arguments to pass to the
            event consumer.
    """
    def __init__(self, config, success_channel, error_channel, **kwargs):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel
        self.kwargs = kwargs

    def _validate_config(self):
        errors = []
        # req keys: keyfile, project, topic, subscription
        # TODO (lynn): keyfile won't be required once we support other
        #              auth methods
        if not self.config.get('keyfile'):
            msg = ('The path to a Service Account JSON keyfile is required to '
                   'authenticate for Google Cloud Pub/Sub.')
            errors.append(msg)

        if not self.config.get('project'):
            msg = ('The GCP project where Cloud Pub/Sub is located is '
                   'required.')
            errors.append(msg)

        if not self.config.get('topic'):
            msg = ('A topic for the client to subscribe to in Cloud Pub/Sub is '
                   'required.')
            errors.append(msg)

        if not self.config.get('subscription'):
            msg = ('A subscription for the client to pull messages from in '
                   'Cloud Pub/Sub is required.')
            errors.append(msg)

        if errors:
            exp_msg = 'Invalid configuration:\n'
            for error in errors:
                logging.error(error)
                exp_msg += error + '\n'
            raise exceptions.GCPConfigError(exp_msg)

        topic_prefix = f'projects/{self.config["project"]}/topics/'
        if not self.config.get('topic').startswith(topic_prefix):
            self.config['topic'] = f'{topic_prefix}{self.config["topic"]}'

        sub_prefix = f'projects/{self.config["project"]}/subscriptions/'
        if not self.config.get('subscription').startswith(sub_prefix):
            sub = f'{sub_prefix}{self.config["subscription"]}'
            self.config['subscription'] = sub

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

    def _init_subscriber_client(self, auth_client):
        # Silly emulator constraints
        creds = getattr(auth_client, 'creds', None)
        client = pubsub.SubscriberClient(credentials=creds)

        try:
            client.create_subscription(
                self.config['subscription'], self.config['topic'])

        except google_exceptions.AlreadyExists:
            # subscription already exists
            pass

        except google_exceptions.NotFound as e:
            msg = f'Topic "{self.config["topic"]}" does not exist.'
            logging.error(msg, exc_info=e)
            raise exceptions.GCPGordonError(msg)

        except Exception as e:
            sub = self.config['subscription']
            msg = f'Error trying to create subscription "{sub}": {e}'
            logging.error(msg, exc_info=e)
            raise exceptions.GCPGordonError(msg)

        logging.info(f'Starting a "{self.config["subscription"]}" subscriber '
                     f'to "{self.config["topic"]}" topic.')
        return client.subscribe(self.config['subscription'])

    def build_event_consumer(self):
        self._validate_config()
        validator = validate.MessageValidator()
        parser = parse.MessageParser()
        auth_client = self._init_auth()
        subscription = self._init_subscriber_client(auth_client)
        loop = self.kwargs.get('loop', asyncio.get_event_loop())

        return GPSEventConsumer(
            self.config, subscription, validator, parser, self.success_channel,
            self.error_channel, loop)


class _GPSThread(threading.Thread):
    """Pub/Sub Message-specific thread with its own asyncio event loop.

    .. attention::
        This thread class is not meant to be worked with directly.

    Google's Pub/Sub library is blocking. This thread class allows us
    to create work in another thread/loop and not block the main event
    loop within Gordon.

    Inspiration directly from https://stackoverflow.com/a/29302684

    Args:
        event (threading.Event): event for thread instance to await
            signal (via ``event.wait()``) to start work.
        name (str): thread name
        daemon (bool): whether the thread should be daemonized or not.
            Defaults to ``False``.
    """
    def __init__(self, event, name=None, daemon=False):
        super().__init__(name=name, daemon=daemon)
        self.loop = None
        self.thread_id = None
        self.event = event

    def run(self):
        """Create and start thread's own event loop."""
        self.loop = asyncio.new_event_loop()
        self.thread_id = threading.current_thread()
        self.loop.call_soon(self.event.set)
        self.loop.run_forever()

    def stop(self):
        """Stop the thread's event loop. Thread safe."""
        self.loop.call_soon_threadsafe(self.loop.stop)

    def add_task(self, coro):
        """Schedule a task to the thread's event loop. Thread safe.

        Args:
            coro: an asyncio `coroutine <https://docs.python.org/3/
                library/asyncio-task.html#coroutine>`_ function
        """
        f = functools.partial(asyncio.ensure_future, coro, loop=self.loop)
        self.loop.call_soon_threadsafe(f)


@zope.interface.implementer(interfaces.IEventConsumerClient)
class GPSEventConsumer:
    """Consume messages from Google Cloud Pub/Sub.

    Pub/Sub messages are continually consumed via google-cloud-python's
    `pubsub` module using gRPC. Every consumed message will create an
    asyncio task that handles the message schema validation, creation
    of a :class:`GEventMessage` instance (`event_msg`), and the
    forwarding on for further processing via the
    :obj:`self.success_channel`. The `pubsub` module handles the message
    ack deadline extension behind the scenes. Once the `event_msg` is
    done processing, gordon's core routing system will submit it back to
    this consumer to be `ack`'ed via the `cleanup` method.

    Args:
        config (dict): configuration relevant to Cloud Pub/Sub.
        success_channel (asyncio.Queue): a sink for successfully
            processed :interface:`interfaces.IEventMessages`.
        error_channel (asyncio.Queue): a sink for
            :interface:`interfaces.IEventMessages` that were not
            processed due to problems.
    """
    phase = 'consume'

    def __init__(self, config, subscriber, validator, parser, success_channel,
                 error_channel, loop, **kwargs):
        self.success_channel = success_channel
        # NOTE: error channel not yet used, but may be in future
        self.error_channel = error_channel
        self._subscriber = subscriber
        self._subscription = config['subscription']
        self._validator = validator
        self._parser = parser
        self._message_schemas = validator.schemas.keys()
        self._loop = loop
        self._logger = logging.getLogger('')
        self._threads = {}

    async def cleanup(self, event_msg):
        """Ack Pub/Sub message and update event message history.

        Args:
            event_msg (GEventMessage): message to clean up
        """
        msg_logger = _utils.GEventMessageLogger(
            self._logger, {'msg_id': event_msg.msg_id})

        msg_logger.debug(f'Acking message.')
        # "blocking" method but just puts msg on a request queue that
        # google.cloud.pubsub manages with threads+grpc
        event_msg._pubsub_msg.ack()

        msg = 'Acknowledged message in Pub/Sub.'
        event_msg.append_to_history(msg, self.phase)

        event, thread = self._threads[event_msg._pubsub_msg.message_id]
        event.set()
        thread.stop()

        msg_logger.info(f'Message is done processing.')

    async def update_phase(self, event_msg, phase=None):
        """Update event message phase to completed phase.

        Updating the message's phase signals that the message has
        completed this phase, and the gordon core router logic should
        pass it onto the next phase (e.g. "enrich").

        Args:
            event_msg (GEventMessage): event message to update.
            phase (str): Optional: phase to update message. Defaults to
                "consume".
        """
        old_phase = event_msg.phase
        event_msg.phase = phase or self.phase
        msg = f'Updated phase from "{old_phase}" to "{event_msg.phase}".'
        event_msg.append_to_history(msg, self.phase)

    def _create_gevent_msg(self, pubsub_msg, data, schema):
        log_entry = f'Created a "{schema}" message.'
        msg = GEventMessage(pubsub_msg, data)
        msg.append_to_history(log_entry, self.phase)
        return msg

    def _get_and_validate_pubsub_msg_schema(self, pubsub_msg_data):
        for _schema in self._message_schemas:
            try:
                self._validator.validate(pubsub_msg_data, _schema)
                return _schema
            except exceptions.InvalidMessageError as e:
                continue

    async def _handle_pubsub_msg(self, pubsub_msg):
        msg_id = pubsub_msg.message_id
        msg_logger = _utils.GEventMessageLogger(
            self._logger, {'msg_id': msg_id})
        msg_logger.info('Received new message.')

        try:
            data = json.loads(pubsub_msg.data)
            msg_logger.debug(f'Received data: {data}')
        except json.JSONDecodeError as e:
            msg = ('Issue loading message data as JSON. '
                   f'Given data: {pubsub_msg.data}')
            msg_logger.warn(f'{msg}')
            pubsub_msg.ack()
            return

        schema = self._get_and_validate_pubsub_msg_schema(data)
        if schema is None:
            msg_logger.warn('No schema found for message received, acking.')
            pubsub_msg.ack()
            return

        msg_logger.debug(f'Message is valid for "{schema}" schema.')

        event_msg_data = self._parser.parse(data, schema)
        event_msg = self._create_gevent_msg(
            pubsub_msg, event_msg_data, schema)

        # if the message has resource records, assume it has all info
        # it needs to be published, and therefore is already enriched
        phase = 'enrich' if event_msg_data['resourceRecords'] else 'consume'
        msg_logger.debug(f'Updating message phase to "{phase}".')
        await self.update_phase(event_msg, phase=phase)

        msg_logger.debug(f'Adding message to the success channel.')
        await self.success_channel.put(event_msg)

    def _thread_pubsub_msg(self, pubsub_msg):
        # Create a new thread with its own event loop to process the
        # pubsub message.
        event = threading.Event()
        thread_name = f'GPSThread_msg-id_{pubsub_msg.message_id}'
        thread = _GPSThread(event, name=thread_name, daemon=True)
        thread.start()
        # Block signal to the thread before start accepting tasks
        # Let the loop's signal us, rather than sleeping
        event.wait()
        thread.add_task(self._handle_pubsub_msg(pubsub_msg))

        # save thread to stop/clean up during self.cleanup
        self._threads[pubsub_msg.message_id] = (event, thread)

    def _manage_subs(self):
        # TODO (lynn): look into needing flow control, e.g.:
        # flow_control = pubsub_v1.types.FlowControl(max_messages=10)
        # self._subscriber.subscribe(self._subscription,
        #     flow_control=flow_control)
        # NOTE: automatically extends deadline in the background;
        #       must `nack()` if can't finish. We don't proactively
        #       `nack` in this plugin since it'll just get redelivered.
        #       A dead process will also timeout the message, with which
        #       it will redeliver.
        future = self._subscriber.open(self._thread_pubsub_msg)

        try:
            # we're running in a threadpool because this is blocking
            future.result()
        except Exception as e:
            self._subscriber.close()
            logging.error(f'Issue polling subscription: {e}', exc_info=e)
            raise exceptions.GCPGordonError(e)

    async def start(self):
        """Start consuming messages from Google Pub/Sub.

        Once a Pub/Sub message is validated, a :class:`GEventMessage`
        is created for the message, then passed to the
        :obj:`self.success_channel` to be handled by an IEnricher
        plugin.
        """
        main_loop = asyncio.get_event_loop()
        # so many threads, name this so it's identifiable
        pfx = 'ThreadPoolExecutor-GPSEventConsumer'
        # NOTE: there should only be one thread pool executor worker
        #       from here since this method is only called once from
        #       gordon core, so there _should_ be no need to limit
        #       workers
        executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix=pfx)
        coro = main_loop.run_in_executor(executor, self._manage_subs)
        await asyncio.gather(coro, loop=main_loop, return_exceptions=True)
