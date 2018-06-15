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
import json
import logging

import pytest

from gordon_gcp.plugins.janitor import gpubsub_publisher
from tests.unit import conftest


@pytest.fixture
def kwargs(config, mock_pubsub_client):
    return {
        'config': {
            'topic': f'projects/{config["project"]}/topics/{config["topic"]}'
        },
        'publisher': mock_pubsub_client,
        'changes_channel': asyncio.Queue()
    }


@pytest.fixture
def gpubsub_publisher_inst(monkeypatch, kwargs):
    return gpubsub_publisher.GPubsubPublisher(**kwargs)


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


@pytest.mark.asyncio
async def test_publish(gpubsub_publisher_inst):
    """Publish received messages."""
    datetime.datetime = conftest.MockDatetime
    msg1 = {'message': 'one'}

    await gpubsub_publisher_inst.publish(msg1)

    msg1['timestamp'] = datetime.datetime.utcnow().isoformat()
    bytes_msg1 = bytes(json.dumps(msg1), encoding='utf-8')

    gpubsub_publisher_inst.publisher.publish.assert_called_once_with(
        gpubsub_publisher_inst.topic, bytes_msg1)
    assert 1 == len(gpubsub_publisher_inst._messages)


@pytest.mark.parametrize('raises,exp_log_records', [
    [False, 1],
    [Exception('foo'), 2],
])
@pytest.mark.asyncio
async def test_run(raises, exp_log_records, gpubsub_publisher_inst,
                   auth_client, mocker, monkeypatch, caplog):
    """Start consuming the changes channel queue."""
    caplog.set_level(logging.DEBUG)

    if raises:
        gpubsub_publisher_inst.publisher.publish.side_effect = Exception('foo')

    msg1 = {'message': 'one'}
    await gpubsub_publisher_inst.changes_channel.put(msg1)
    await gpubsub_publisher_inst.changes_channel.put(None)

    await gpubsub_publisher_inst.run()

    gpubsub_publisher_inst.publisher.publish.assert_called_once()
    assert exp_log_records == len(caplog.records)
