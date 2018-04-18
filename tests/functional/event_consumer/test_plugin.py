# -*- coding: utf-8 -*-
#
# Copyright 2018 Spotify AB
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
import logging
import os
import threading
from unittest import mock

import asyncio_extras
import pytest
from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub

from gordon_gcp import plugins

from tests.functional import conftest

MOD_PATCH = 'gordon_gcp.plugins.event_consumer'
DATETIME_PATCH = f'{MOD_PATCH}.datetime.datetime'
HERE = os.path.abspath(os.path.dirname(__file__))
JSON_FIXTURES = os.path.join(HERE, 'fixtures')


@pytest.fixture
def msg_happy_audit_log():
    with open(os.path.join(JSON_FIXTURES, 'msg.audit-log.json'), 'r') as f:
        return json.load(f)


@pytest.fixture
def msg_happy_audit_log_parsed(msg_happy_audit_log):
    return {
        'timestamp': msg_happy_audit_log['timestamp'],
        'resourceRecords': [],
        'resourceName': msg_happy_audit_log['protoPayload']['resourceName'],
        'action': 'additions'
    }


def get_publisher_client(config):
    # don't publish to actual pubsub!
    if not os.environ.get('PUBSUB_EMULATOR_HOST'):
        logging.error('Emulator is not running!')
        raise SystemExit(1)

    topic = config['topic']
    client = pubsub.PublisherClient()
    try:
        client.create_topic(topic)
    except google_exceptions.AlreadyExists:
        # already created
        pass
    except Exception as e:
        logging.error(f'Error trying to create topic "{topic}": {e}.')
        raise SystemExit(1)
    return client


async def run_event_consumer(config, consumer, message_fixture):
    print('sleeping')
    await asyncio.sleep(.5)

    print('starting consumer')
    with mock.patch(DATETIME_PATCH, conftest.MockDatetime):
        await consumer.start()


@asyncio_extras.contextmanager.async_contextmanager
async def route_message(client, gevent_msg):
    await client.cleanup(gevent_msg)
    yield


def publish_message(config, message_fixture):
    client = get_publisher_client(config)
    bytes_msg = bytes(json.dumps(message_fixture), encoding='utf-8')
    print('publishing a message')
    future = client.publish(config['topic'], bytes_msg)

    return future.result()  # message ID


@pytest.mark.asyncio
async def test_consume_audit_log(msg_happy_audit_log, msg_happy_audit_log_parsed, event_loop):
    """Happy path for an audit log message."""
    config = {
        'core': {},
        # would be under [gcp.event_consumer] in toml file
        'keyfile': '/not/a/real/key.json',
        'topic': 'projects/a-project/topics/test-topic',
        'subscription': 'projects/a-project/subscriptions/test-sub',
        'project': 'a-project',
    }
    published_message_id = publish_message(config, msg_happy_audit_log)

    success, error = asyncio.Queue(), asyncio.Queue()
    consumer = plugins.get_event_consumer(config, success, error)
    task = asyncio.ensure_future(run_event_consumer(config, consumer, msg_happy_audit_log))

    await asyncio.sleep(2)
    assert 1 == consumer.success_channel.qsize()
    gevent_msg = await consumer.success_channel.get()

    assert published_message_id == gevent_msg.msg_id
    assert msg_happy_audit_log_parsed == gevent_msg.data
    assert 'consume' == gevent_msg.phase

    exp_history_log = [
        {
            'message': 'Created a "audit-log" message.',
            'plugin': 'consume',
            'timestamp': '2018-01-01T11:30:00.000000Z',
        }, {
            'message': 'Updated phase from "None" to "consume".',
            'plugin': 'consume',
            'timestamp': '2018-01-01T11:30:00.000000Z',
        }
    ]
    assert exp_history_log == gevent_msg.history_log

    task.set_result('tests passed')

    async with route_message(consumer, gevent_msg):
        assert consumer.success_channel.empty()
        exp_log = {
            'message': 'Acknowledged message in Pub/Sub.',
            'plugin': 'consume',
            'timestamp': '2018-01-01T11:30:00.000000Z',
        }
        assert exp_log in gevent_msg.history_log

    for thread in threading.enumerate():
        if 'GPSEventConsumer' in thread.name:
            thread._tstate_lock.release()
            break
