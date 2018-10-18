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

import copy
import logging

import aiohttp
import attr
import pytest
from aioresponses import aioresponses

from gordon_gcp.clients import auth
from gordon_gcp.clients import gdns


logging.getLogger('asyncio').setLevel(logging.WARNING)


@pytest.fixture
def rrset_dict():
    return {
        'name': 'test',
        'type': 'A',
        'rrdatas': ['10.1.2.3'],
        'ttl': 500,
        'kind': 'dns#resourceRecordSet'
    }


def test_create_gcp_rrset(rrset_dict):
    """Create valid GCPResourceRecordSet instances."""
    rrset = gdns.GCPResourceRecordSet(**rrset_dict)
    assert rrset_dict == attr.asdict(rrset)


def test_create_gcp_rrset_no_ttl(rrset_dict):
    # default TTL when not provided
    data = rrset_dict.copy()
    data.pop('ttl')
    rrset = gdns.GCPResourceRecordSet(**data)
    data['ttl'] = 300
    assert data == attr.asdict(rrset)


def test_create_gcp_rrset_raises():
    # Raise when required params are missing
    missing_params = {
        'name': 'test'
    }
    with pytest.raises(TypeError):
        gdns.GCPResourceRecordSet(**missing_params)


@pytest.fixture
def client(mocker, create_mock_coro):
    auth_client = mocker.Mock(auth.GAuthClient)
    creds = mocker.Mock()
    auth_client.creds = creds
    session = aiohttp.ClientSession()

    client = gdns.GDNSClient(
        'a-project', auth_client=auth_client, session=session)

    # mock out inherited methods from http.AIOConnection
    get_json_mock, get_json_coro = create_mock_coro()
    request_post_mock, post_request_coro = create_mock_coro()
    client.get_json = get_json_coro
    client._get_json_mock = get_json_mock

    client.request = post_request_coro
    client._request_post_mock = request_post_mock
    yield client
    # test teardown
    client._session.close()


def test_dns_client_default(client):
    assert 'a-project' == client.project


def test_get_rrsets_as_objects(rrset_dict):
    exp = [gdns.GCPResourceRecordSet(**rrset_dict)]
    assert exp == gdns.GDNSClient.get_rrsets_as_objects([rrset_dict])


@pytest.mark.asyncio
@pytest.mark.parametrize('params', [
    None,
    {'fields': 'some-other-field,another-field'},
    {'name': 'example.com.', 'type': 'A'}
])
async def test_get_records_for_zone(fake_response_data, client, caplog,
                                    params):
    mock_get_json_called = False
    mock_get_json_params = []
    # get_records_for_zone() modifies its params argument in place; see
    # https://en.wikipedia.org/wiki/Evaluation_strategy#Call_by_sharing
    # https://jeffknupp.com/blog/2012/11/13/is-python-callbyvalue-or-callbyreference-neither/
    original_params = copy.deepcopy(params)

    async def mock_get_json(*args, **kwargs):
        nonlocal mock_get_json_called
        nonlocal mock_get_json_params
        # without the copy, both entries in mock_get_json_params change; because
        # of late binding, they both point to the final value inside this
        # inner function
        mock_get_json_params.append(copy.deepcopy(kwargs.get('params')))
        data = fake_response_data.copy()
        if not mock_get_json_called:
            data['nextPageToken'] = 1
            mock_get_json_called = True
        return data

    client.get_json = mock_get_json

    url = f'{client._base_url}/managedZones/a-zone/rrsets'
    with aioresponses() as mocked:
        mocked.get(url, status=200)
        # paginated requests
        mocked.get(url, status=200)
        records = await client.get_records_for_zone('a.zone.', params=params)
        actual_params1 = {
            'fields': ('rrsets/name,rrsets/kind,rrsets/rrdatas,'
                       'rrsets/type,rrsets/ttl,nextPageToken')
        }
        if original_params:
            actual_params1.update(original_params)
        actual_params2 = copy.deepcopy(actual_params1)
        actual_params2['pageToken'] = 1
        assert [actual_params1, actual_params2] == mock_get_json_params
        assert 6 == len(records)
        exp = fake_response_data['rrsets'] + fake_response_data['rrsets']
        assert exp == records

    assert 1 == len(caplog.records)


@pytest.mark.parametrize('dns_zone,exp_managed_zone', [
    ('example.com.', 'example-com'),
    ('20.10.in-addr.arpa.', 'reverse-20-10'),
    ('30.20.10.in-addr.arpa.', 'reverse-20-10'),
    ('40.30.20.10.in-addr.arpa.', 'reverse-20-10')
])
def test_get_managed_zone(dns_zone, exp_managed_zone, client):
    assert exp_managed_zone == client.get_managed_zone(dns_zone)


@pytest.mark.parametrize('dns_zone,prefix,expected', [
    ('example.com.', 'production', 'production-example-com'),
    ('20.10.in-addr.arpa.', 'production', 'production-reverse-20-10'),
    ('30.20.10.in-addr.arpa.', 'testing', 'testing-reverse-20-10'),
    ('40.30.20.10.in-addr.arpa.', 'testing', 'testing-reverse-20-10')
])
def test_get_managed_zone_with_prefix(mocker, dns_zone, prefix, expected):
    client = gdns.GDNSClient('a-project', auth_client=mocker.Mock(),
                             session=mocker.Mock(), default_zone_prefix=prefix)
    assert expected == client.get_managed_zone(dns_zone)


@pytest.mark.asyncio
@pytest.mark.parametrize('resp,expected', [
    ({'status': 'pending'}, False),
    ({'status': 'done'}, True)
])
async def test_is_change_done(client, resp, expected):
    client._get_json_mock.return_value = resp
    actual = await client.is_change_done('example.com.', 'some-id-12')
    assert expected == actual

    exp_url = f'{client._base_url}/managedZones/example-com/changes/some-id-12'
    client._get_json_mock.assert_called_with(exp_url)


@pytest.mark.asyncio
async def test_publish_changes(client, rrset_dict):
    expected = 'some-id-12345'
    client._request_post_mock.return_value = f'{{"id": "{expected}"}}'

    changes = {'kind': 'dns#change', 'additions': [rrset_dict]}
    actual = await client.publish_changes('example.com.', changes)
    assert expected == actual

    exp_url = f'{client._base_url}/managedZones/example-com/changes'
    client._request_post_mock.assert_called_with('post', exp_url, json=changes)
