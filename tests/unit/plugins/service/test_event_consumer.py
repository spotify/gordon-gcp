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
import json
import threading

import pytest
from google.cloud import pubsub
from google.cloud import pubsub_v1
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins.service import event_consumer
from gordon_gcp.schema import parse
from gordon_gcp.schema import validate
from tests.unit import conftest


MOD_PATCH = 'gordon_gcp.plugins.service.event_consumer'
DATETIME_PATCH = f'{MOD_PATCH}.datetime.datetime'
NEW_EV_PATCH = f'{MOD_PATCH}.asyncio.new_event_loop'


#####
# GEventMessage tests
#####
@pytest.fixture(scope='session')
def raw_msg_data(creation_audit_log_data):
    return {
        'protoPayload': {
            'methodName': 'v1.compute.instances.insert',
            'resourceName': creation_audit_log_data['resourceName'],
        },
        'timestamp': '2017-12-04T20:13:51.414016721Z',
    }


@pytest.fixture
def pubsub_msg(mocker, raw_msg_data):
    pubsub_msg = mocker.MagicMock(pubsub_v1.subscriber.message.Message)
    pubsub_msg.message_id = 1234
    pubsub_msg.data = bytes(json.dumps(raw_msg_data), encoding='utf-8')
    return pubsub_msg


@pytest.fixture
def thread(mocker, monkeypatch):
    mock = mocker.Mock(threading.Thread)
    patch = f'{MOD_PATCH}.threading.Thread'
    monkeypatch.setattr(patch, mock)
    return mock


@pytest.fixture
def event(mocker):
    mock = mocker.Mock(threading.Event)
    return mock


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


def test_event_msg_append_to_history(mocker, pubsub_msg, raw_msg_data):
    """Write entry to event message history log."""
    mocker.patch(DATETIME_PATCH, conftest.MockDatetime)
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


def test_event_msg_update_phase(mocker, pubsub_msg, raw_msg_data):
    """Update phase of the message and write to history log."""
    mocker.patch(DATETIME_PATCH, conftest.MockDatetime)
    msg = event_consumer.GEventMessage(pubsub_msg, raw_msg_data)

    msg.update_phase('emo')

    assert 'emo' == msg.phase
    expected = [{
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': None,
        'message': 'Updated phase from None to emo'
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
    patch = f'{MOD_PATCH}.parse.MessageParser'
    monkeypatch.setattr(patch, mock)
    return mock


@pytest.fixture
def validator(mocker, monkeypatch):
    mock = mocker.MagicMock(validate.MessageValidator)
    schemas = {'schema1': 'a-schema', 'schema2': 'another-schema'}
    mock.schemas = schemas
    patch = f'{MOD_PATCH}.validate.MessageValidator'
    monkeypatch.setattr(patch, mock)
    return mock


@pytest.fixture
def subscriber_client(mocker, monkeypatch):
    mock = mocker.MagicMock(pubsub.SubscriberClient)
    patch = f'{MOD_PATCH}.pubsub.SubscriberClient'
    monkeypatch.setattr(patch, mock)
    return mock.return_value


@pytest.fixture
def flow_control(mocker, monkeypatch):
    mock = mocker.Mock()
    patch = f'{MOD_PATCH}.types.FlowControl'
    monkeypatch.setattr(patch, mock)
    return mock


def test_event_consumer_default(mocker, subscriber_client, flow_control,
                                validator, parser, event_loop, channel_pair,
                                metrics):
    """GPSEventConsumer implements IEventConsumerClient"""
    config = {'subscription': '/projects/test-project/subscriptions/test-sub'}
    success, error = channel_pair
    client = event_consumer.GPSEventConsumer(
        config, success, error, metrics, subscriber_client, flow_control,
        validator, parser, event_loop)

    assert interfaces.IRunnable.providedBy(client)
    assert interfaces.IRunnable.implementedBy(event_consumer.GPSEventConsumer)
    assert interfaces.IMessageHandler.providedBy(client)
    assert interfaces.IMessageHandler.implementedBy(
        event_consumer.GPSEventConsumer)
    assert subscriber_client is client._subscriber
    assert config['subscription'] is client._subscription
    assert validator is client._validator
    assert ['schema1', 'schema2'] == list(client._message_schemas)
    assert success is client.success_channel
    assert error is client.error_channel
    assert metrics is client.metrics
    assert 'consume' == client.start_phase
    assert 'cleanup' == client.phase
    assert event_loop is client._loop


@pytest.fixture
def consumer(subscriber_client, flow_control, validator, event_loop,
             channel_pair, metrics):
    config = {'subscription': '/projects/test-project/subscriptions/test-sub'}
    success, error = channel_pair
    parser = parse.MessageParser()
    client = event_consumer.GPSEventConsumer(
        config, success, error, metrics, subscriber_client, flow_control,
        validator, parser, event_loop)
    return client


@pytest.mark.asyncio
async def test_run(consumer):
    """Consumer starts consuming from a Pub/Sub subscription."""
    await consumer.run()

    consumer._subscriber.subscribe.assert_called_once_with(
        consumer._subscription,
        consumer._schedule_pubsub_msg,
        flow_control=consumer._flow_control
    )


def test_manage_subs(consumer):
    consumer._manage_subs()
    exp_callback = consumer._schedule_pubsub_msg
    consumer._subscriber.subscribe.assert_called_once_with(
        consumer._subscription,
        exp_callback,
        flow_control=consumer._flow_control
    )


def test_manage_subs_raises(consumer, caplog):
    msg = 'foo'
    exc = Exception(msg)
    consumer._subscriber.subscribe.return_value.result.side_effect = [exc]

    with pytest.raises(exceptions.GCPGordonError, match=msg):
        consumer._manage_subs()

    exp_callback = consumer._schedule_pubsub_msg
    consumer._subscriber.subscribe.assert_called_once_with(
        consumer._subscription,
        exp_callback,
        flow_control=consumer._flow_control
    )
    consumer._subscriber.close.assert_called_once_with()

    assert 1 == len(caplog.records)


class CustomLoop:
    def __init__(self, mocker, event_loop):
        self._call_soon_ts_mock = mocker.Mock()
        self._event_loop = event_loop
        self._task_list = []

    def create_task(self, coro_or_future):
        return self._event_loop.create_task(coro_or_future)

    def call_soon_threadsafe(self, *args, **kwargs):
        self._call_soon_ts_mock(*args, **kwargs)
        task = args[0]
        self._task_list.append(task())


@pytest.mark.asyncio
async def test_schedule_pubsub_msg(pubsub_msg, consumer, mocker, monkeypatch,
                                   event_loop):

    custom_loop = CustomLoop(mocker, event_loop)
    monkeypatch.setattr(consumer, '_loop', custom_loop)

    async def _mock(*args, **kwargs):
        await asyncio.sleep(0)

    monkeypatch.setattr(consumer, '_handle_pubsub_msg', _mock)

    consumer._schedule_pubsub_msg(pubsub_msg)

    custom_loop._call_soon_ts_mock.assert_called_once()
    assert 1 == len(custom_loop._task_list)

    # to avoid "task pending destroyed" warnings from pytest
    await custom_loop._task_list[0]


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
                                 creation_audit_log_data, caplog, pubsub_msg,
                                 mock_get_and_validate, mock_create_gevent_msg):
    """Validate pubsub msg, create GEventMessage, and add to success chnl."""
    event_msg = event_consumer.GEventMessage(
        pubsub_msg, creation_audit_log_data, phase='consume')
    mock_create_gevent_msg.return_value = event_msg

    mock_run_coro_threadsafe = mocker.Mock()
    patch = ('gordon_gcp.plugins.service.event_consumer.asyncio.'
             'run_coroutine_threadsafe')
    monkeypatch.setattr(patch, mock_run_coro_threadsafe)

    await consumer._handle_pubsub_msg(pubsub_msg)

    creation_audit_log_data.update({'resourceRecords': []})
    mock_get_and_validate.assert_called_once_with(raw_msg_data)
    mock_create_gevent_msg.assert_called_once_with(
        pubsub_msg, creation_audit_log_data, 'audit-log')

    assert 4 == len(caplog.records)
    assert 'consume' == event_msg.phase
    mock_run_coro_threadsafe.assert_called_once()


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


def test_create_gevent_msg(mocker, pubsub_msg, raw_msg_data, consumer):
    """Create an instance of GEventMessage with a pubsub message."""
    mocker.patch(DATETIME_PATCH, conftest.MockDatetime)
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
                                            side_effect, expected, consumer):
    """Validate & return schema of a pubsub message's data."""
    validator.validate.side_effect = side_effect

    data = json.loads(pubsub_msg.data)
    schema = consumer._get_and_validate_pubsub_msg_schema(data)

    calls = [mocker.call(data, 'schema1')]
    if not side_effect[0]:
        calls.append(mocker.call(data, 'schema2'))
    validator.validate.assert_has_calls(calls)

    assert expected == schema


@pytest.mark.asyncio
async def test_handle_message(mocker, consumer, caplog, pubsub_msg):
    """Consumer acks Pub/Sub message."""
    mocker.patch(DATETIME_PATCH, conftest.MockDatetime)

    event_msg = event_consumer.GEventMessage(pubsub_msg, {})

    await consumer.handle_message(event_msg)

    pubsub_msg.ack.assert_called_once_with()

    exp_entry = {
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': 'cleanup',
        'message': 'Acknowledged message in Pub/Sub.',
    }
    assert exp_entry in event_msg.history_log
    assert 2 == len(caplog.records)
