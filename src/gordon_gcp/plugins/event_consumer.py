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

import datetime
import json
import logging

import zope.interface
from google.cloud import pubsub  # NOQA
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins import _utils
from gordon_gcp.schema import parse  # NOQA
from gordon_gcp.schema import validate  # NOQA


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

    async def cleanup(self, event_msg):
        """Ack Pub/Sub message and update event message history.

        Args:
            event_msg (GEventMessage): message to clean up
        """
        msg_logger = _utils.GEventMessageLogger(
            self._logger, {'msg_id': event_msg.msg_id})

        msg_logger.info(f'Acking message.')
        # "blocking" method but just puts msg on a request queue that
        # google.cloud.pubsub manages with threads+grpc
        event_msg._pubsub_msg.ack()

        msg = 'Acknowledged message in Pub/Sub.'
        event_msg.append_to_history(msg, self.phase)
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
            msg_logger.info(f'Received data: {data}')
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

        msg_logger.info(f'Message is valid for "{schema}" schema.')

        event_msg_data = self._parser.parse(data, schema)
        event_msg = self._create_gevent_msg(pubsub_msg, event_msg_data, schema)

        # if the message has resource records, assume it has all info
        # it needs to be published, and therefore is already enriched
        phase = 'enrich' if event_msg_data['resourceRecords'] else 'consume'
        msg_logger.info(f'Updating message phase to "{phase}".')
        await self.update_phase(event_msg, phase=phase)

        msg_logger.info(f'Adding message to the success channel.')
        await self.success_channel.put(event_msg)

    def _create_handle_pubsub_msg_task(self, pubsub_msg):
        self._loop.create_task(self._handle_pubsub_msg(pubsub_msg))

    async def start(self):
        """Start consuming messages from Google Pub/Sub.

        Once a Pub/Sub message is validated, a :class:`GEventMessage`
        is created for the message, then passed to the
        :obj:`self.success_channel` to be handled by an IEnricher
        plugin.
        """
        # TODO (lynn): look into needing flow control, e.g.:
        # flow_control = pubsub_v1.types.FlowControl(max_messages=10)
        # self._subscriber.subscribe(self._subscription,
        #     flow_control=flow_control)

        subscription = self._subscriber.subscribe(self._subscription)
        # NOTE: automatically extends deadline in the background;
        #       must `nack()` if can't finish.
        subscription.open(self._create_handle_pubsub_msg_task)
