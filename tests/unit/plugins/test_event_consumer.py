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

import asyncio

import pytest  # NOQA
from gordon import interfaces

from gordon_gcp.plugins import event_consumer


def test_implements_interface():
    """GPSEventConsumer implements IEventConsumerClient"""
    config = {'foo': 'bar'}
    success, error = asyncio.Queue(), asyncio.Queue()
    client = event_consumer.GPSEventConsumer(config, success, error)

    assert interfaces.IEventConsumerClient.providedBy(client)
    assert interfaces.IEventConsumerClient.implementedBy(
        event_consumer.GPSEventConsumer)
    assert config is client.config
    assert success is client.success_channel
    assert error is client.error_channel
    assert 'consume' == client.phase
