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

import logging

import aiohttp
import attr
import pytest
from aioresponses import aioresponses

from gordon_gcp.clients import auth
from gordon_gcp.clients import gdns


logging.getLogger('asyncio').setLevel(logging.WARNING)


def test_create_gcp_rrset():
    """Create valid GCPResourceRecordSet instances."""
    data = {
        'name': 'test',
        'type': 'A',
        'rrdatas': ['10.1.2.3'],
        'ttl': 500,
        'kind': 'dns#resourceRecordSet'
    }
    rrset = gdns.GCPResourceRecordSet(**data)
    assert data == attr.asdict(rrset)

    # default TTL when not provided
    data.pop('ttl')
    rrset = gdns.GCPResourceRecordSet(**data)
    data['ttl'] = 300
    assert data == attr.asdict(rrset)

    # Raise when required params are missing
    missing_params = {
        'name': 'test'
    }
    with pytest.raises(TypeError):
        gdns.GCPResourceRecordSet(**missing_params)


def test_dns_client_default(mocker):
    auth_client = mocker.Mock(auth.GAuthClient)
    creds = mocker.Mock()
    auth_client.creds = creds
    session = aiohttp.ClientSession()

    client = gdns.GDNSClient(
        'a-project', auth_client, session=session)

    assert 'a-project' == client.project

    client._session.close()


@pytest.fixture
def client(mocker):
    auth_client = mocker.Mock(auth.GAuthClient)
    creds = mocker.Mock()
    auth_client.creds = creds
    session = aiohttp.ClientSession()
    client = gdns.GDNSClient(
        'a-project', auth_client=auth_client, session=session)
    yield client
    # test teardown
    client._session.close()


@pytest.mark.asyncio
async def test_get_records_for_zone(fake_response_data, client, caplog,
                                    monkeypatch):
    mock_get_json_called = 0

    async def mock_get_json(*args, **kwargs):
        nonlocal mock_get_json_called
        data = fake_response_data.copy()
        if not mock_get_json_called:
            data['nextPageToken'] = 1
        mock_get_json_called += 1
        return data

    monkeypatch.setattr(client, 'get_json', mock_get_json)

    url = f'{client._base_url}/managedZones/a-zone/rrsets'
    with aioresponses() as mocked:
        mocked.get(url, status=200)
        # paginated requests
        mocked.get(url, status=200)
        records = await client.get_records_for_zone('a-zone')

        assert all(
            [isinstance(r, gdns.GCPResourceRecordSet) for r in records])
        assert 6 == len(records)

    assert 1 == len(caplog.records)
