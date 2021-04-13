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
from unittest import mock

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


@pytest.fixture
def rrset_dict_after_conversion(rrset_dict):
    rrset_dict['source'] = None
    rrset_dict['rrdatas'] = tuple(rrset_dict['rrdatas'])
    return rrset_dict


def test_create_gcp_rrset(rrset_dict, rrset_dict_after_conversion):
    """Create valid ResourceRecordSet instances."""
    rrset = reconciler.ResourceRecordSet(**rrset_dict)
    assert rrset_dict_after_conversion == vars(rrset)


def test_create_gcp_rrset_no_ttl(rrset_dict, rrset_dict_after_conversion):
    # default TTL when not provided
    data = rrset_dict.copy()
    data.pop('ttl')
    rrset = reconciler.ResourceRecordSet(**data)
    rrset_dict_after_conversion['ttl'] = 300
    assert rrset_dict_after_conversion == vars(rrset)


def test_create_gcp_rrset_raises():
    # Raise when required params are missing
    missing_params = {
        'name': 'test'
    }
    with pytest.raises(TypeError):
        reconciler.ResourceRecordSet(**missing_params)


def test_rrset_inequality(rrset_dict):
    other_rrset_dict = rrset_dict.copy()
    other_rrset_dict['name'] = 'someothername.com.'
    rrset = reconciler.ResourceRecordSet(**rrset_dict)
    other_rrset = reconciler.ResourceRecordSet(**other_rrset_dict)
    assert rrset != other_rrset


def test_rrset_repr(rrset_dict):
    rrset = reconciler.ResourceRecordSet(**rrset_dict)
    expected = ("{'name': 'test', 'type': 'A', 'rrdatas': ('10.1.2.3',), "
                "'kind': 'dns#resourceRecordSet', 'ttl': 500, 'source': None}")
    assert expected == repr(rrset)


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
async def recon_client(config, dns_client, metrics):
    rch, chch = asyncio.Queue(), asyncio.Queue()
    recon_client = reconciler.GDNSReconciler(
        config, metrics, dns_client, rch, chch)
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
    assert coro1.done()
    assert coro2.done()

    assert 1 == recon_client.changes_channel.qsize()


@pytest.mark.asyncio
async def test_publish_change_messages(recon_client, fake_response_data,
                                       caplog):
    """Publish message to changes queue."""
    rrsets = fake_response_data['rrsets']
    desired_rrsets = [reconciler.ResourceRecordSet(**kw) for kw in rrsets]

    await recon_client.publish_change_messages(desired_rrsets)

    assert 3 == recon_client.changes_channel.qsize()
    assert 4 == len(caplog.records)


@pytest.fixture
def soa_ns_rrsets():
    return [
        {
            'name': 'example.net.',
            'type': 'SOA',
            'ttl': 300,
            'rrdatas': [
                'ns-cloud-c1.googledomains.com. '
                'cloud-dns-hostmaster.google.com. 2 21600 3600 259200 300',
            ],
            'kind': 'dns#resourceRecordSet'
        },
        {
            'name': 'example.net.',
            'type': 'NS',
            'ttl': 300,
            'rrdatas': [
                'ns1.example.net.',
            ],
            'kind': 'dns#resourceRecordSet'
        },
        {
            'name': 'z-test.example.net.',
            'type': 'NS',
            'ttl': 300,
            'rrdatas': [
                'ns47.example.net',
            ],
            'kind': 'dns#resourceRecordSet'
        }
    ]


def test_remove_soa_and_root_ns(fake_response_data, soa_ns_rrsets):
    rrsets = fake_response_data['rrsets']
    rrsets_all = rrsets + soa_ns_rrsets

    rrsets_expected = rrsets + [soa_ns_rrsets[2]]
    expected = set(reconciler.ResourceRecordSet(**rr) for rr in rrsets_expected)
    actual = reconciler.GDNSReconciler.create_rrset_set(
        'example.net.', rrsets_all)
    assert expected == actual


@pytest.fixture
def extra_rrset():
    return {
        'name': 'foo.example.net.',
        'type': 'A',
        'ttl': 300,
        'rrdatas': ['1.2.3.255'],
        'kind': 'dns#resourceRecordSet',
        'source': 'gdns'
    }


@pytest.fixture
def duplicate_rrset(soa_ns_rrsets):
    dupe = soa_ns_rrsets[2].copy()
    dupe['source'] = 'someotherauthority'
    return dupe


@pytest.mark.parametrize('has_desired,has_someotherauthority', [
    (True, True),
    (True, False),
    (False, False)])
@pytest.mark.asyncio
async def test_validate_rrsets_by_zone(recon_client, fake_response_data,
                                       soa_ns_rrsets, extra_rrset, has_desired,
                                       caplog, monkeypatch, duplicate_rrset,
                                       has_someotherauthority):
    """Differences are detected and returned."""
    rrsets = fake_response_data['rrsets'] + soa_ns_rrsets
    expected_missing_rrset = rrsets[0]
    if has_someotherauthority:
        rrsets = [duplicate_rrset] + rrsets
    else:
        rrsets = rrsets + [duplicate_rrset]

    mock_get_records_for_zone_called = 0

    return_rrsets = []
    for rrset in fake_response_data['rrsets']:
        rr = rrset.copy()
        rr['source'] = 'gdns'
        return_rrsets.append(rr)
    return_rrsets[0]['rrdatas'] = ['10.4.5.6']
    return_rrsets.append(extra_rrset)

    async def mock_get_records_for_zone(*args, **kwargs):
        nonlocal mock_get_records_for_zone_called
        mock_get_records_for_zone_called += 1
        return return_rrsets

    recon_client.dns_client.get_records_for_zone = mock_get_records_for_zone

    input_rrsets = []
    expected_missing_rrsets = set()
    expected_extra_rrsets = set()
    if has_desired:
        for rrset in rrsets:
            input_rrset = rrset.copy()
            input_rrset['source'] = rrset.get('source', 'gceauthority')
            input_rrsets.append(input_rrset)

        expected_missing_rrsets = set(
            reconciler.ResourceRecordSet(**record)
            for record in [expected_missing_rrset, soa_ns_rrsets[2]]
        )
        expected_extra_rrsets = set(
            reconciler.ResourceRecordSet(**record)
            for record in [return_rrsets[0], extra_rrset]
        )
    actual_missing_rrsets, actual_extra_rrsets = (
        await recon_client.validate_rrsets_by_zone(
            'example.net.', input_rrsets))

    assert expected_missing_rrsets == actual_missing_rrsets
    assert expected_extra_rrsets == actual_extra_rrsets
    assert 2 == len(caplog.records)
    assert 1 == mock_get_records_for_zone_called


args = 'msg,exp_log_records,exp_mock_calls,qsize,additions,deletions'

params = [
    # happy path
    [{'zone': 'example.net.', 'rrsets': []}, 4, 1, 2, 0, 1],
    # full happy path - ugly but can't import conftest.py from here
    [{'zone': 'example.net.', 'rrsets': 'FAKE'}, 7, 1, 5, 3, 1],
    # no rrsets key
    [{'zone': 'example.net.'}, 3, 0, 1, None, None],
    # no zone key
    [{'rrsets': []}, 3, 0, 1, None, None],
]


@pytest.mark.asyncio
@pytest.mark.parametrize(args, params)
async def test_run(msg, exp_log_records, exp_mock_calls, qsize, additions,
                   deletions, fake_response_data, extra_rrset, caplog,
                   recon_client, monkeypatch):
    """Start reconciler & continue if certain errors are raised."""
    mock_validate_rrsets_by_zone_called = 0

    async def mock_validate_rrsets_by_zone(zone, rrsets):
        nonlocal mock_validate_rrsets_by_zone_called
        mock_validate_rrsets_by_zone_called += 1
        return (rrsets, [reconciler.ResourceRecordSet(**extra_rrset)])

    monkeypatch.setattr(
        recon_client, 'validate_rrsets_by_zone', mock_validate_rrsets_by_zone)

    if 'rrsets' in msg and msg['rrsets'] == 'FAKE':
        msg['rrsets'] = [
            reconciler.ResourceRecordSet(**rrset)
            for rrset in fake_response_data['rrsets']
        ]
    await recon_client.rrset_channel.put(msg)
    await recon_client.rrset_channel.put(None)

    await recon_client.run()

    assert qsize == recon_client.changes_channel.qsize()
    assert exp_log_records == len(caplog.records)
    assert exp_mock_calls == mock_validate_rrsets_by_zone_called
    context = {'plugin': 'reconciler'}
    recon_client.metrics._timer_mock.assert_called_once_with(
        'plugin-runtime', context=context)
    recon_client.metrics.timer_stub.start_mock.assert_called_once_with()
    recon_client.metrics.timer_stub.stop_mock.assert_called_once_with()
    context['source'] = 'unknown'
    if additions:
        context['action'] = 'additions'
        recon_client.metrics._set_mock.assert_has_calls(
            [mock.call(
                'rrsets-handled', additions, context=context)])
    if deletions:
        context['source'] = 'gdns'
        context['action'] = 'deletions'
        recon_client.metrics._set_mock.assert_has_calls(
            [mock.call(
                'rrsets-handled', deletions, context=context)])
