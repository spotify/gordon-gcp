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

import zope.interface
from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub
from google.cloud.pubsub_v1 import types
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
            the GCEEnricher, or to be published via the GDNSPublisher.
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

    def update_phase(self, new_phase):
        self.phase, old_phase = new_phase, self.phase
        msg = f'Updated phase from {old_phase} to {new_phase}'
        self.append_to_history(msg, plugin=None)
        logging.info(msg)


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
    def __init__(self, config, success_channel, error_channel, metrics,
                 **kwargs):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel
        self.metrics = metrics
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

        if ('max_msg_age' in self.config and
                (not isinstance(self.config['max_msg_age'], int) or
                    self.config['max_msg_age'] < 1)):
            msg = ('Invalid value for max_msg_age (discard messages older than '
                   'this many seconds).')
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

        max_messages = self.config.get('max_messages', 25)
        flow_control = types.FlowControl(max_messages=max_messages)

        logging.info(f'Starting a "{self.config["subscription"]}" subscriber '
                     f'to "{self.config["topic"]}" topic.')

        return client, flow_control

    def build_event_consumer(self):
        self._validate_config()
        validator = validate.MessageValidator()
        parser = parse.MessageParser()
        auth_client = self._init_auth()
        subscriber, flow_control = self._init_subscriber_client(auth_client)
        if not self.kwargs.get('loop'):
            self.kwargs['loop'] = asyncio.get_event_loop()

        return GPSEventConsumer(
            self.config, self.success_channel, self.error_channel,
            self.metrics, subscriber, flow_control, validator, parser,
            **self.kwargs
        )


@zope.interface.implementer(interfaces.IRunnable, interfaces.IMessageHandler)
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
    this consumer to be `ack`'ed via the `handle_method` method used for
    pubsub cleanup.

    Args:
        config (dict): configuration relevant to Cloud Pub/Sub.
        success_channel (asyncio.Queue): a sink for successfully
            processed :interface:`interfaces.IEventMessages`.
        error_channel (asyncio.Queue): a sink for
            :interface:`interfaces.IEventMessages` that were not
            processed due to problems.
    """
    start_phase = 'consume'
    phase = 'cleanup'

    def __init__(self, config, success_channel, error_channel, metrics,
                 subscriber, flow_control, validator, parser, loop,
                 **kwargs):
        self.success_channel = success_channel
        # NOTE: error channel not yet used, but may be in future
        self.error_channel = error_channel
        self.metrics = metrics
        self._subscriber = subscriber
        self._flow_control = flow_control
        self._subscription = config['subscription']
        self._validator = validator
        self._parser = parser
        self._message_schemas = validator.schemas.keys()
        self._loop = loop
        self._logger = logging.getLogger('')
        self._max_msg_age = config.get('max_msg_age', 300)

    async def handle_message(self, event_msg):
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

        msg_logger.info(f'Message is done processing.')

    def _create_gevent_msg(self, pubsub_msg, data, schema):
        log_entry = f'Created a "{schema}" message.'
        msg = GEventMessage(pubsub_msg, data, phase=self.start_phase)
        msg.append_to_history(log_entry, self.start_phase)
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

        msg_datetime = pubsub_msg.publish_time
        # need a TZ-aware object in UTC
        cur_datetime = datetime.datetime.now(datetime.timezone.utc)
        msg_age = (cur_datetime - msg_datetime).total_seconds()
        if msg_age > self._max_msg_age:
            msg_logger.warn(f'Message is too old ({msg_age} seconds), '
                            'acking and discarding.')
            context = {
                'msg_id': msg_id,
                'msg_age': msg_age
            }
            await self.metrics.incr('msg-too-old', context=context)
            pubsub_msg.ack()
            return

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

        msg_logger.debug(f'Adding message to the success channel.')

        coro = self.success_channel.put(event_msg)
        asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _schedule_pubsub_msg(self, pubsub_msg):
        # schedule future back on the main event loop thread
        coro = self._handle_pubsub_msg(pubsub_msg)
        task = functools.partial(asyncio.ensure_future, coro, loop=self._loop)
        # we're in another thread here separate from the main event loop
        self._loop.call_soon_threadsafe(task)

    def _manage_subs(self):
        # NOTE: automatically extends deadline in the background;
        #       must `nack()` if can't finish. We don't proactively
        #       `nack` in this plugin since it'll just get redelivered.
        #       A dead process will also timeout the message, with which
        #       it will redeliver.
        future = self._subscriber.subscribe(
            self._subscription,
            self._schedule_pubsub_msg,
            flow_control=self._flow_control
        )

        try:
            # we're running in a threadpool because this is blocking
            future.result()
        except Exception as e:
            self._subscriber.close()
            logging.error(f'Issue polling subscription: {e}', exc_info=e)
            raise exceptions.GCPGordonError(e)

    async def run(self):
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
