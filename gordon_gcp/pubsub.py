# -*- coding: utf-8 -*-
#
# Copyright 2017 Spotify AB
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

import zope.interface
from gordon import interfaces


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
