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
from gordon_gcp.plugins import event_consumer
from gordon_gcp.schema import parse
from gordon_gcp.schema import validate
from tests.unit import conftest


MOD_PATCH = 'gordon_gcp.plugins.event_consumer'
DATETIME_PATCH = f'{MOD_PATCH}.datetime.datetime'
NEW_EV_PATCH = f'{MOD_PATCH}.asyncio.new_event_loop'


#####
# GEventMessage tests
#####
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
    return mock.subscribe.return_value


def test_event_consumer_default(subscriber_client, validator, parser,
                                event_loop, channel_pair):
    """GPSEventConsumer implements IEventConsumerClient"""
    config = {'subscription': '/projects/test-project/subscriptions/test-sub'}
    success, error = channel_pair
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
def consumer(subscriber_client, validator, event_loop, channel_pair):
    config = {'subscription': '/projects/test-project/subscriptions/test-sub'}
    success, error = channel_pair
    parser = parse.MessageParser()
    client = event_consumer.GPSEventConsumer(
        config, subscriber_client, validator, parser, success, error,
        event_loop)
    return client


@pytest.mark.asyncio
async def test_start(consumer):
    """Consumer starts consuming from a Pub/Sub subscription."""
    await consumer.start()

    consumer._subscriber.open.assert_called_once_with(
        consumer._thread_pubsub_msg)


def test_manage_subs(consumer):
    consumer._manage_subs()

    exp_callback = consumer._thread_pubsub_msg
    consumer._subscriber.open.assert_called_once_with(exp_callback)


def test_manage_subs_raises(consumer, caplog):
    msg = 'foo'
    exc = Exception(msg)
    consumer._subscriber.open.return_value.result.side_effect = [exc]

    with pytest.raises(exceptions.GCPGordonError, match=msg):
        consumer._manage_subs()

    exp_callback = consumer._thread_pubsub_msg
    consumer._subscriber.open.assert_called_once_with(exp_callback)
    consumer._subscriber.close.assert_called_once_with()

    assert 1 == len(caplog.records)


def test_thread_pubsub_msg(consumer, event, pubsub_msg, mocker, monkeypatch):
    thread = mocker.Mock(event_consumer._GPSThread)
    monkeypatch.setattr(f'{MOD_PATCH}._GPSThread', thread)
    monkeypatch.setattr(f'{MOD_PATCH}.threading.Event', event)

    async def noop(*args, **kwargs):
        await asyncio.sleep(0)

    monkeypatch.setattr(consumer, '_handle_pubsub_msg', lambda x: noop)

    consumer._thread_pubsub_msg(pubsub_msg)

    thread.assert_called_once_with(
        event.return_value, 'GPSThread_msg-id_1234', daemon=True)

    ret_thread, ret_event = thread.return_value, event.return_value
    ret_thread.start.assert_called_once_with()
    ret_event.wait.assert_called_once_with()
    ret_thread.add_task.assert_called_once_with(
        consumer._handle_pubsub_msg(pubsub_msg))
    assert ret_event, ret_thread == consumer._threads[pubsub_msg.message_id]


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


def test_create_gevent_msg(mocker, pubsub_msg, raw_msg_data, validator):
    """Create an instance of GEventMessage with a pubsub message."""
    mocker.patch(DATETIME_PATCH, conftest.MockDatetime)
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


@pytest.mark.asyncio
@pytest.mark.parametrize('init_phase', (None, 'emo'))
async def test_update_phase(init_phase, mocker, consumer, pubsub_msg):
    """Phase of a GEventMessage instance is updated."""
    mocker.patch(DATETIME_PATCH, conftest.MockDatetime)
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


@pytest.mark.asyncio
async def test_cleanup(mocker, consumer, caplog, pubsub_msg):
    """Consumer acks Pub/Sub message."""
    mocker.patch(DATETIME_PATCH, conftest.MockDatetime)

    event_msg = event_consumer.GEventMessage(pubsub_msg, {})
    mock_event = mocker.Mock(threading.Event)
    mock_thread = mocker.Mock(event_consumer._GPSThread)
    consumer._threads[event_msg.msg_id] = (mock_event, mock_thread)

    await consumer.cleanup(event_msg)

    pubsub_msg.ack.assert_called_once_with()

    exp_entry = {
        'timestamp': '2018-01-01T11:30:00.000000Z',
        'plugin': 'consume',
        'message': 'Acknowledged message in Pub/Sub.',
    }
    assert exp_entry in event_msg.history_log
    mock_event.set.assert_called_once_with()
    mock_thread.stop.assert_called_once_with()
    assert 2 == len(caplog.records)


#####
# _GPSThread tests
#####
def test_create_gpsthread(thread, event):
    gps_thread = event_consumer._GPSThread(event)

    assert not gps_thread.loop
    assert not gps_thread.thread_id
    assert event is gps_thread.event


class CustomLoop(asyncio.BaseEventLoop):
    def run_forever(self):
        # so we don't _actually_ run forever
        pass

    def stop(self):
        pass

    def call_soon_threadsafe(self, *args, **kwargs):
        pass


@pytest.fixture
def custom_event_loop():
    return CustomLoop()


def test_gpsthread_run(thread, event, custom_event_loop, monkeypatch):
    monkeypatch.setattr(NEW_EV_PATCH, lambda: custom_event_loop)

    gps_thread = event_consumer._GPSThread(event)
    gps_thread.run()

    assert custom_event_loop is gps_thread.loop
    current_id = threading.current_thread()
    assert current_id == gps_thread.thread_id

    # cleanup
    gps_thread.stop()


def test_gpsthread_stop(thread, event, custom_event_loop):
    gps_thread = event_consumer._GPSThread(event)
    gps_thread.loop = custom_event_loop

    gps_thread.stop()

    assert not gps_thread.thread_id
    assert not gps_thread.loop.is_running()


# NOTE: This test will emit a warning about the `noop` task not being
#       awaited. This is okay since we're not testing that bit
@pytest.mark.filterwarnings('ignore:coroutine')
def test_gpsthread_add_task(mocker, monkeypatch):
    mock_loop = mocker.Mock(asyncio.BaseEventLoop)
    monkeypatch.setattr(NEW_EV_PATCH, mock_loop)

    event = threading.Event()
    gps_thread = event_consumer._GPSThread(event)
    gps_thread.start()

    async def noop():
        await asyncio.sleep(0)

    gps_thread.add_task(noop())

    gps_thread.loop.call_soon_threadsafe.assert_called_once()

    # cleanup
    gps_thread.stop()
