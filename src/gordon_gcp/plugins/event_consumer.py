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

import zope.interface
from gordon import interfaces


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
        self.msg_id = pubsub_msg._ack_id
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
    """Consume events from Google Cloud Pub/Sub.

    Args:
        config (dict): configuration relevant to Cloud Pub/Sub.
        success_channel (asyncio.Queue): a sink for successfully
            processed :interface:`interfaces.IEventMessages`.
        error_channel (asyncio.Queue): a sink for
            :interface:`interfaces.IEventMessages` that were not
            processed due to problems.
    """
    phase = 'consume'

    def __init__(self, config, success_channel, error_channel):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel

    def start(self):
        # do something
        pass  # pragma: no cover

    def cleanup(self):
        # do something
        pass  # pragma: no cover
