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
import concurrent.futures
import datetime
import logging
from unittest import mock

import pytest

from gordon_gcp.plugins.janitor import gpubsub_publisher
from tests.unit import conftest


@pytest.fixture
def gpubsub_publisher_inst(config, mock_pubsub_client, metrics):
    config['topic'] = f'projects/{config["project"]}/topics/{config["topic"]}'
    client = gpubsub_publisher.GPubsubPublisher(
        config, metrics, mock_pubsub_client, asyncio.Queue())
    return client


@pytest.mark.parametrize('side_effect', [
    # tasks completed on first retry
    (False, True),
    # tasks completed before timeout
    (False, False, True),
    # tasks did not complete before timeout
    (False, False, False),
])
@pytest.mark.asyncio
async def test_cleanup(side_effect, gpubsub_publisher_inst, monkeypatch,
                       mocker, create_mock_coro):
    """Proper cleanup with or without pending tasks."""

    mock_sleep, mock_sleep_coro = create_mock_coro()
    patch = 'gordon_gcp.plugins.janitor.gpubsub_publisher.asyncio.sleep'
    monkeypatch.setattr(patch, mock_sleep_coro)
    gpubsub_publisher_inst.cleanup_timeout = 1

    mock_msg1 = mocker.Mock(concurrent.futures.Future)
    mock_msg2 = mocker.Mock(concurrent.futures.Future)

    if side_effect:
        mock_msg1.done.side_effect = side_effect
        mock_msg2.done.side_effect = side_effect

    gpubsub_publisher_inst._messages.add(mock_msg1)
    gpubsub_publisher_inst._messages.add(mock_msg2)

    await gpubsub_publisher_inst.cleanup()

    if not any(side_effect):
        mock_msg1.cancel.assert_called_once()
        mock_msg2.cancel.assert_called_once()
    else:
        mock_msg1.cancel.assert_not_called()
        mock_msg2.cancel.assert_not_called()

    assert len(side_effect) == mock_msg1.done.call_count

    # Expect to call sleep for every false returned from done inside loop
    exp_sleep_calls = len([b for b in side_effect if not b]) - 1
    assert exp_sleep_calls == mock_sleep.call_count

    assert 0 == gpubsub_publisher_inst.changes_channel.qsize()


@pytest.mark.parametrize('publish_completed', (True, False))
@pytest.mark.asyncio
async def test_publish(gpubsub_publisher_inst, publish_completed, mocker):
    """Publish received messages."""
    datetime.datetime = conftest.MockDatetime
    msg1 = {
        'action': 'additions',
        'resourceRecords': {
            'name': 'a.b.com.',
            'rrdatas': ('10.11.12.13',)
        }
    }

    expected_num_messages = 0
    if not publish_completed:
        expected_num_messages = 1
        gpubsub_publisher_inst.publisher.publish = mocker.Mock()

    await gpubsub_publisher_inst.publish(msg1)

    msg1['timestamp'] = datetime.datetime.utcnow().isoformat()
    bytes_msg1 = bytes(
        '{"action": "additions", "resourceRecords": {"name": "a.b.com.", '
        '"rrdatas": ["10.11.12.13"]}, "timestamp": "2018-01-01T11:30:00"}',
        encoding='utf-8')

    gpubsub_publisher_inst.publisher.publish.assert_called_once_with(
        gpubsub_publisher_inst.topic, bytes_msg1)
    assert expected_num_messages == len(gpubsub_publisher_inst._messages)


@pytest.mark.parametrize('raises', [
    False,
    Exception('foo'),
])
@pytest.mark.asyncio
async def test_run(raises, gpubsub_publisher_inst,
                   auth_client, mocker, monkeypatch, caplog):
    """Start consuming the changes channel queue."""
    caplog.set_level(logging.DEBUG)

    if raises:
        gpubsub_publisher_inst.publisher.publish.side_effect = Exception('foo')

    msg1 = {'action': 'additions', 'resourceRecords': {'name': 'a.b.com.'}}
    await gpubsub_publisher_inst.changes_channel.put(msg1)
    await gpubsub_publisher_inst.changes_channel.put(None)

    await gpubsub_publisher_inst.run()

    gpubsub_publisher_inst.publisher.publish.assert_called_once()
    context = {'plugin': 'gpubsub-publisher'}
    gpubsub_publisher_inst.metrics._timer_mock.assert_called_once_with(
        'plugin-runtime', context=context)
    context['action'] = 'additions'
    start_mock = gpubsub_publisher_inst.metrics.timer_stub.start_mock
    start_mock.assert_called_once_with()

    assert 0 == len(gpubsub_publisher_inst._messages)

    assert 2 == len(caplog.records)
    assert 'Finished sending' in str(caplog.records.pop())
    if raises:
        assert 'Exception' in str(caplog.records.pop())
        context['result'] = 'error'
        gpubsub_publisher_inst.metrics._incr_mock.assert_has_calls(
            [mock.call('change-message', value=1, context=context)])
    else:
        stop_mock = gpubsub_publisher_inst.metrics.timer_stub.stop_mock
        stop_mock.assert_called_once_with()
        assert 'Message published' in str(caplog.records.pop())
        context['result'] = 'published'
        gpubsub_publisher_inst.metrics._incr_mock.assert_has_calls(
            [mock.call('change-message', value=1, context=context)])
