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
def kwargs(config, gpubsub_publisher_client):
    return {
        'config': config,
        'publisher': gpubsub_publisher_client,
        'changes_channel': asyncio.Queue()
    }


@pytest.mark.parametrize('exp_log_records,timeout,side_effect', [
    # tasks did not complete before timeout
    [2, 0, (False, False)],
    # tasks completed
    [1, 1, (False, True)],
    # tasks completed before timeout
    [1, 1, (False, False, True)],
])
@pytest.mark.asyncio
async def test_cleanup(exp_log_records, timeout, side_effect, kwargs,
                       gpubsub_publisher_client, auth_client, caplog, mocker,
                       monkeypatch):
    """Proper cleanup with or without pending tasks."""
    caplog.set_level(logging.DEBUG)

    mock_msg1 = mocker.Mock(concurrent.futures.Future)
    mock_msg2 = mocker.Mock(concurrent.futures.Future)

    if side_effect:
        mock_msg1.done.side_effect = side_effect
        mock_msg2.done.side_effect = side_effect

    kwargs['config']['cleanup_timeout'] = timeout
    client = gpubsub_publisher.GPubsubPublisher(**kwargs)
    client._messages.add(mock_msg1)
    client._messages.add(mock_msg2)

    await client.cleanup()

    assert exp_log_records == len(caplog.records)
    if exp_log_records == 2:
        mock_msg1.cancel.assert_called_once()
        mock_msg2.cancel.assert_called_once()

    assert 0 == client.changes_channel.qsize()


@pytest.mark.asyncio
async def test_publish(kwargs, gpubsub_publisher_client, auth_client, mocker,
                       monkeypatch):
    """Publish received messages."""
    datetime.datetime = conftest.MockDatetime

    topic = kwargs['config']['topic']
    project = kwargs['config']['project']
    exp_topic = f'projects/{project}/topics/{topic}'
    kwargs['config']['topic'] = exp_topic

    client = gpubsub_publisher.GPubsubPublisher(**kwargs)

    msg1 = {'message': 'one'}

    await client.publish(msg1)

    msg1['timestamp'] = datetime.datetime.utcnow().isoformat()
    bytes_msg1 = bytes(json.dumps(msg1), encoding='utf-8')

    gpubsub_publisher_client.publish.assert_called_once_with(
        exp_topic, bytes_msg1)
    assert 1 == len(client._messages)


@pytest.mark.parametrize('raises,exp_log_records', [
    [False, 1],
    [Exception('foo'), 2],
])
@pytest.mark.asyncio
async def test_run(raises, exp_log_records, kwargs, gpubsub_publisher_client,
                   auth_client, mocker, monkeypatch, caplog):
    """Start consuming the changes channel queue."""
    caplog.set_level(logging.DEBUG)

    if raises:
        gpubsub_publisher_client.publish.side_effect = [Exception('foo')]

    msg1 = {'message': 'one'}
    await kwargs['changes_channel'].put(msg1)
    await kwargs['changes_channel'].put(None)

    client = gpubsub_publisher.GPubsubPublisher(**kwargs)
    await client.run()

    gpubsub_publisher_client.publish.assert_called_once()
    assert exp_log_records == len(caplog.records)
