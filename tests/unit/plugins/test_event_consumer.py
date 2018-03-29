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
import functools
import json

import pytest
from google.cloud import pubsub
from google.cloud import pubsub_v1
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins import event_consumer
from gordon_gcp.schema import parse
from gordon_gcp.schema import validate


#####
# GEventMessage tests
#####
@pytest.fixture(scope='session')
def audit_log_data():
    resource_name = ('projects/123456789101/zones/us-central1-c/instances/'
                     'an-instance-name-b34c')
    return {
        'action': 'additions',
        'resourceName': resource_name,
        'timestamp': '2017-12-04T20:13:51.414016721Z'
    }


@pytest.fixture(scope='session')
def raw_msg_data(audit_log_data):
    return {
        'protoPayload': {
            'methodName': 'v1.compute.instances.insert',
            'resourceName': audit_log_data['resourceName'],
        },
        'timestamp': '2017-12-04T20:13:51.414016721Z',
    }


@pytest.fixture
def pubsub_msg(mocker, raw_msg_data):
    pubsub_msg = mocker.MagicMock(pubsub_v1.subscriber.message.Message)
    pubsub_msg.message_id = 1234
    pubsub_msg.data = bytes(json.dumps(raw_msg_data), encoding='utf-8')
    return pubsub_msg


@pytest.mark.parametrize('phase', [None, 'emo'])
def test_implements_message_interface(phase, pubsub_msg, raw_msg_data):
    """GEventMessage implements IEventMessage."""
    args = [pubsub_msg, raw_msg_data]
    if phase:
        args.append(phase)

    msg = event_consumer.GEventMessage(*args)

    assert pubsub_msg is msg._pubsub_msg
    assert pubsub_msg.message_id is msg.msg_id
    assert raw_msg_data == msg.data
    assert not msg.history_log
    assert phase == msg.phase


# pytest prevents monkeypatching datetime directly
class MockDatetime(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return datetime.datetime(2018, 1, 1, 11, 30, 0)


def patch_datetime(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        datetime.datetime = MockDatetime
        return func(*args, **kwargs)
    return wrapper


@patch_datetime
def test_event_msg_append_to_history(pubsub_msg, raw_msg_data):
    """Write entry to event message history log."""
    msg = event_consumer.GEventMessage(pubsub_msg, raw_msg_data)
    entry_msg = 'an entry'
    plugin = 'test-plugin'
    msg.append_to_history(entry_msg, plugin)

    expected = [{
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': plugin,
        'message': entry_msg
    }]

    assert expected == msg.history_log


#####
# GPSEventConsumer tests
#####
@pytest.fixture
def parser(mocker, monkeypatch, raw_msg_data):
    mock = mocker.MagicMock(parse.MessageParser)
    data = {
        'action': 'additions',
        'resourceName': raw_msg_data['protoPayload']['resourceName'],
        'resourceRecords': None,
    }
    mock.parse.return_value = data
    patch = 'gordon_gcp.plugins.event_consumer.parse.MessageParser'
    monkeypatch.setattr(patch, mock)
    return mock


@pytest.fixture
def validator(mocker, monkeypatch):
    mock = mocker.MagicMock(validate.MessageValidator)
    schemas = {'schema1': 'a-schema', 'schema2': 'another-schema'}
    mock.schemas = schemas
    patch = 'gordon_gcp.plugins.event_consumer.validate.MessageValidator'
    monkeypatch.setattr(patch, mock)
    return mock


@pytest.fixture
def subscriber_client(mocker, monkeypatch):
    mock = mocker.MagicMock(pubsub.SubscriberClient)
    patch = 'gordon_gcp.plugins.event_consumer.pubsub.SubscriberClient'
    monkeypatch.setattr(patch, mock)
    return mock


def test_event_consumer_default(subscriber_client, validator, parser,
                                event_loop):
    """GPSEventConsumer implements IEventConsumerClient"""
    config = {'subscription': '/projects/test-project/subscriptions/test-sub'}
    success, error = asyncio.Queue(), asyncio.Queue()
    client = event_consumer.GPSEventConsumer(
        config, subscriber_client, validator, parser, success, error,
        event_loop)

    assert interfaces.IEventConsumerClient.providedBy(client)
    assert interfaces.IEventConsumerClient.implementedBy(
        event_consumer.GPSEventConsumer)
    assert subscriber_client is client._subscriber
    assert config['subscription'] is client._subscription
    assert validator is client._validator
    assert ['schema1', 'schema2'] == list(client._message_schemas)
    assert success is client.success_channel
    assert error is client.error_channel
    assert 'consume' == client.phase
    assert event_loop is client._loop


@pytest.fixture
def consumer(subscriber_client, validator, event_loop):
    config = {'subscription': '/projects/test-project/subscriptions/test-sub'}
    success, error = asyncio.Queue(), asyncio.Queue()
    parser = parse.MessageParser()
    client = event_consumer.GPSEventConsumer(
        config, subscriber_client, validator, parser, success, error,
        event_loop)
    return client


@pytest.mark.asyncio
async def test_start(consumer):
    """Consumer starts consuming from a Pub/Sub subscription."""
    await consumer.start()

    sub = '/projects/test-project/subscriptions/test-sub'
    consumer._subscriber.subscribe.assert_called_once_with(sub)
    consumer._subscriber.subscribe.return_value.open.assert_called_once_with(
        consumer._create_handle_pubsub_msg_task)


@pytest.mark.asyncio
async def test_create_handle_pubsub_msg_task(consumer, event_loop):
    """Create an asyncio task for handling a pubsub message."""
    consumer._create_handle_pubsub_msg_task({})

    tasks = asyncio.Task.all_tasks(loop=event_loop)
    # one of the tasks is the actual test itself running in
    # pytest-asyncio's event_loop; the test needs to be ran in the
    # event_loop, so it can await the task created by
    # _create_handle_pubsub_msg_task
    assert 2 == len(tasks)
    names = [t._coro.__name__ for t in tasks]
    assert '_handle_pubsub_msg' in names


@pytest.fixture
def mock_get_and_validate(consumer, mocker, monkeypatch):
    mock = mocker.MagicMock(return_value='audit-log')
    monkeypatch.setattr(consumer, '_get_and_validate_pubsub_msg_schema', mock)
    return mock


@pytest.fixture
def mock_create_gevent_msg(consumer, mocker, monkeypatch):
    mock = mocker.MagicMock()
    monkeypatch.setattr(consumer, '_create_gevent_msg', mock)
    return mock


@pytest.mark.asyncio
async def test_handle_pubsub_msg(mocker, monkeypatch, consumer, raw_msg_data,
                                 audit_log_data, caplog, pubsub_msg,
                                 mock_get_and_validate, mock_create_gevent_msg):
    """Validate pubsub msg, create GEventMessage, and add to success chnl."""
    event_msg = event_consumer.GEventMessage(pubsub_msg, audit_log_data, [])
    mock_create_gevent_msg.return_value = event_msg

    await consumer._handle_pubsub_msg(pubsub_msg)

    audit_log_data.update({'resourceRecords': []})
    mock_get_and_validate.assert_called_once_with(raw_msg_data)
    mock_create_gevent_msg.assert_called_once_with(
        pubsub_msg, audit_log_data, 'audit-log')
    assert 1 == consumer.success_channel.qsize()
    assert event_msg is await consumer.success_channel.get()
    assert 5 == len(caplog.records)


@pytest.mark.asyncio
async def test_handle_pubsub_msg_json_err(mocker, monkeypatch, consumer, caplog,
                                          pubsub_msg, mock_get_and_validate,
                                          mock_create_gevent_msg):
    """Ack an invalidate JSON pubsub msg."""
    pubsub_msg.data = bytes('invalid json', encoding='utf-8')

    await consumer._handle_pubsub_msg(pubsub_msg)

    pubsub_msg.ack.assert_called_once_with()
    mock_get_and_validate.assert_not_called()
    mock_create_gevent_msg.assert_not_called()
    assert 0 == consumer.success_channel.qsize()
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_handle_pubsub_msg_invalid(mocker, monkeypatch, consumer, caplog,
                                         pubsub_msg,
                                         mock_get_and_validate,
                                         mock_create_gevent_msg):
    """Ack an invalidate pubsub msg."""
    consumer._get_and_validate_pubsub_msg_schema.return_value = None

    await consumer._handle_pubsub_msg(pubsub_msg)

    pubsub_msg.ack.assert_called_once_with()
    mock_create_gevent_msg.assert_not_called()
    assert 0 == consumer.success_channel.qsize()
    assert 3 == len(caplog.records)


@patch_datetime
def test_create_gevent_msg(pubsub_msg, raw_msg_data, validator):
    """Create an instance of GEventMessage with a pubsub message."""
    config = {'subscription': 'foo'}
    # bare bones consumer
    consumer = event_consumer.GPSEventConsumer(
        config, None, validator, None, None, None, None)

    msg = consumer._create_gevent_msg(pubsub_msg, raw_msg_data, 'a-schema')

    assert isinstance(msg, event_consumer.GEventMessage)

    exp_entry = {
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': 'consume',
        'message': 'Created a "a-schema" message.',
    }
    assert exp_entry in msg.history_log


@pytest.mark.parametrize('side_effect, expected', [
    # schema1 is valid
    ((True, exceptions.InvalidMessageError()), 'schema1'),
    # schema2 is valid
    ((exceptions.InvalidMessageError(), True), 'schema2'),
    # no valid schemas
    ((exceptions.InvalidMessageError(), exceptions.InvalidMessageError()),
     None),
])
def test_get_and_validate_pubsub_msg_schema(mocker, pubsub_msg, validator,
                                            side_effect, expected):
    """Validate & return schema of a pubsub message's data."""
    config = {'subscription': 'foo'}
    parser = parse.MessageParser()
    consumer = event_consumer.GPSEventConsumer(
        config, None, validator, parser, None, None, None)

    validator.validate.side_effect = side_effect
    data = json.loads(pubsub_msg.data)
    schema = consumer._get_and_validate_pubsub_msg_schema(data)

    calls = [mocker.call(data, 'schema1')]
    if not side_effect[0]:
        calls.append(mocker.call(data, 'schema2'))
    validator.validate.assert_has_calls(calls)

    assert expected == schema


@patch_datetime
@pytest.mark.asyncio
@pytest.mark.parametrize('init_phase', (None, 'emo'))
async def test_update_phase(init_phase, mocker, consumer, pubsub_msg):
    """Phase of a GEventMessage instance is updated."""
    event_msg = event_consumer.GEventMessage(pubsub_msg, {}, phase=init_phase)

    assert init_phase == event_msg.phase  # sanity check
    await consumer.update_phase(event_msg)

    assert 'consume' == event_msg.phase
    exp_entry = {
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': 'consume',
        'message': f'Updated phase from "{init_phase}" to "consume".',
    }
    assert exp_entry in event_msg.history_log


@patch_datetime
@pytest.mark.asyncio
async def test_cleanup(mocker, consumer, caplog, pubsub_msg):
    """Consumer acks Pub/Sub message."""
    event_msg = event_consumer.GEventMessage(pubsub_msg, {})

    await consumer.cleanup(event_msg)

    pubsub_msg.ack.assert_called_once_with()

    exp_entry = {
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': 'consume',
        'message': 'Acknowledged message in Pub/Sub.',
    }
    assert exp_entry in event_msg.history_log
    assert 2 == len(caplog.records)
