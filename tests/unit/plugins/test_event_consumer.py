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

import asyncio
import datetime
import json

import pytest
from google.cloud import pubsub_v1
from gordon import interfaces

from gordon_gcp.plugins import event_consumer


#####
# GEventMessage tests
#####
@pytest.fixture
def raw_msg_data():
    return {'some': 'data'}


@pytest.fixture
def pubsub_msg(mocker, raw_msg_data):
    pubsub_msg = mocker.MagicMock(pubsub_v1.subscriber.message.Message)
    pubsub_msg._ack_id = 1234
    pubsub_msg.data = bytes(json.dumps(raw_msg_data), encoding='utf-8')
    return pubsub_msg


@pytest.mark.parametrize('phase', [None, 'emo'])
def test_implements_message_interface(phase, pubsub_msg, raw_msg_data):
    """GEventMessage implements IEventMessage."""
    if phase:
        msg = event_consumer.GEventMessage(pubsub_msg, raw_msg_data, phase)
    else:
        msg = event_consumer.GEventMessage(pubsub_msg, raw_msg_data)

    assert pubsub_msg is msg._pubsub_msg
    assert pubsub_msg._ack_id is msg.msg_id
    assert raw_msg_data == msg.data
    assert not msg.history_log
    assert phase == msg.phase


# pytest prevents monkeypatching datetime directly
class MockDatetime(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return datetime.datetime(2018, 1, 1, 11, 30, 0)


def test_event_msg_append_to_history(pubsub_msg, raw_msg_data):
    """Write entry to event message history log."""
    datetime.datetime = MockDatetime

    msg = event_consumer.GEventMessage(pubsub_msg, raw_msg_data)

    entry_msg = 'an entry'
    plugin = 'test-plugin'
    expected = [{
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': plugin,
        'message': entry_msg
    }]
    msg.append_to_history(entry_msg, plugin)

    assert expected == msg.history_log


#####
# GPSEventConsumer tests
#####
def test_implements_consumer_interface():
    """GPSEventConsumer implements IEventConsumerClient."""
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
