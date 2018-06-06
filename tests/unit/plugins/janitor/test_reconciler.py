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

import aiohttp
import pytest

from gordon_gcp.clients import auth
from gordon_gcp.clients import gdns
from gordon_gcp.plugins.janitor import reconciler


@pytest.fixture
def minimal_config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'scopes': ['my-awesome-scope'],
        'project': 'a-project',
    }


@pytest.fixture
def full_config(minimal_config):
    minimal_config['api_version'] = 'zeta'
    return minimal_config


@pytest.fixture
async def dns_client(mocker, monkeypatch):
    mock = mocker.Mock(gdns.GDNSClient)
    mock._session = aiohttp.ClientSession()
    monkeypatch.setattr(
        'gordon_gcp.plugins.janitor.reconciler.gdns.GDNSClient', mock)
    yield mock
    await mock._session.close()


@pytest.fixture
def config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'project': 'test-example',
        'scopes': ['a-scope'],
    }


@pytest.fixture
def auth_client(mocker, monkeypatch):
    mock = mocker.Mock(auth.GAuthClient)
    monkeypatch.setattr(
        'gordon_gcp.plugins.janitor.reconciler.auth.GAuthClient', mock)
    return mock


args = 'timeout,exp_timeout'
params = [
    (None, 60),
    (30, 30),
]


@pytest.mark.parametrize(args, params)
def test_reconciler_default(timeout, exp_timeout, config, dns_client):
    rrset_chnl, changes_chnl = asyncio.Queue(), asyncio.Queue()

    if timeout:
        config['cleanup_timeout'] = timeout

    recon_client = reconciler.GDNSReconciler(
        config, dns_client, rrset_chnl, changes_chnl)
    assert exp_timeout == recon_client.cleanup_timeout
    assert recon_client.dns_client is not None


@pytest.fixture
async def recon_client(config, dns_client):
    rch, chch = asyncio.Queue(), asyncio.Queue()
    recon_client = reconciler.GDNSReconciler(config, dns_client, rch, chch)
    yield recon_client
    while not chch.empty():
        await chch.get()


args = 'exp_log_records,timeout'
params = [
    # tasks did not complete before timeout
    [2, 0],
    # tasks completed before timeout
    [1, 1],
]


@pytest.mark.parametrize(args, params)
@pytest.mark.asyncio
async def test_cleanup(exp_log_records, timeout, recon_client, caplog, mocker,
                       monkeypatch):
    """Proper cleanup with or without pending tasks."""
    recon_client.cleanup_timeout = timeout

    # mocked methods names must match those in reconciler._ASYNC_METHODS
    async def publish_change_messages():
        await asyncio.sleep(0)

    async def validate_rrsets_by_zone():
        await asyncio.sleep(0)

    coro1 = asyncio.ensure_future(publish_change_messages())
    coro2 = asyncio.ensure_future(validate_rrsets_by_zone())

    mock_task = mocker.MagicMock(asyncio.Task)
    mock_task.all_tasks.side_effect = [
        # in the `while iterations` loop twice
        # timeout of `0` will never hit this loop
        [coro1, coro2],
        [coro1.done(), coro2.done()]
    ]
    monkeypatch.setattr(
        'gordon_gcp.plugins.janitor.reconciler.asyncio.Task', mock_task)

    await recon_client.cleanup()

    assert exp_log_records == len(caplog.records)
    if exp_log_records == 2:
        # it's in a cancelling state which can't be directly tested
        assert not coro1.done()
        assert not coro2.done()
    else:
        assert coro1.done()
        assert coro2.done()

    assert 1 == recon_client.changes_channel.qsize()


@pytest.mark.asyncio
async def test_publish_change_messages(recon_client, fake_response_data,
                                       caplog):
    """Publish message to changes queue."""
    rrsets = fake_response_data['rrsets']
    desired_rrsets = [gdns.GCPResourceRecordSet(**kw) for kw in rrsets]

    await recon_client.publish_change_messages(desired_rrsets)

    assert 3 == recon_client.changes_channel.qsize()
    assert 4 == len(caplog.records)


@pytest.mark.asyncio
async def test_validate_rrsets_by_zone(recon_client, fake_response_data, caplog,
                                       monkeypatch):
    """A difference is detected and a change message is published."""
    rrsets = fake_response_data['rrsets']

    mock_get_records_for_zone_called = 0

    async def mock_get_records_for_zone(*args, **kwargs):
        nonlocal mock_get_records_for_zone_called
        mock_get_records_for_zone_called += 1
        rrsets = fake_response_data['rrsets']
        rrsets[0]['rrdatas'] = ['10.4.5.6']
        return [
            gdns.GCPResourceRecordSet(**kw) for kw in rrsets
        ]

    monkeypatch.setattr(
        recon_client.dns_client, 'get_records_for_zone',
        mock_get_records_for_zone
    )

    await recon_client.validate_rrsets_by_zone('example.net.', rrsets)

    assert 1 == recon_client.changes_channel.qsize()
    assert 3 == len(caplog.records)
    assert 1 == mock_get_records_for_zone_called


args = 'msg,exp_log_records,exp_mock_calls'
params = [
    # happy path
    [{'zone': 'example.net.', 'rrsets': []}, 1, 1],
    # no rrsets key
    [{'zone': 'example.net.'}, 3, 0],
    # no zone key
    [{'rrsets': []}, 3, 0],
]


@pytest.mark.asyncio
@pytest.mark.parametrize(args, params)
async def test_run(msg, exp_log_records, exp_mock_calls, caplog, recon_client,
                   monkeypatch):
    """Start reconciler & continue if certain errors are raised."""
    mock_validate_rrsets_by_zone_called = 0

    async def mock_validate_rrsets_by_zone(*args, **kwargs):
        nonlocal mock_validate_rrsets_by_zone_called
        mock_validate_rrsets_by_zone_called += 1
        await asyncio.sleep(0)

    monkeypatch.setattr(
        recon_client, 'validate_rrsets_by_zone', mock_validate_rrsets_by_zone)

    await recon_client.rrset_channel.put(msg)
    await recon_client.rrset_channel.put(None)

    await recon_client.run()

    assert exp_log_records == len(caplog.records)
    assert exp_mock_calls == mock_validate_rrsets_by_zone_called
