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

import datetime
import json
from unittest import mock

import aiohttp
import pytest
from aioresponses import aioresponses

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.clients import http
from tests.unit import conftest


#####
# Tests for simple client instantiation
#####
@pytest.mark.parametrize('provide_session', [True, False])
def test_http_client_default(provide_session, mocker):
    """AIOConnection is created with expected attributes."""
    session = None
    if provide_session:
        session = aiohttp.ClientSession()

    auth_client = mocker.Mock(auth.GAuthClient)
    auth_client._session = aiohttp.ClientSession()
    creds = mocker.Mock()
    auth_client.creds = creds
    client = http.AIOConnection(auth_client=auth_client, session=session)

    if provide_session:
        assert session is client._session
        assert auth_client._session is not client._session
    else:
        assert auth_client._session is client._session
        assert session is not client._session

    client._session.close()


@pytest.fixture
def client(mocker, auth_client):
    session = aiohttp.ClientSession()
    client = http.AIOConnection(auth_client=auth_client, session=session)
    yield client
    session.close()


args = 'token,expiry,exp_mocked_refresh'
params = [
    # no token - expiry doesn't matter
    [None, datetime.datetime(2018, 1, 1, 9, 30, 0), 1],
    # expired token
    ['0ldc0ffe3', datetime.datetime(2018, 1, 1, 9, 30, 0), 1],
    # token expires within 60 seconds
    ['0ldc0ffe3', datetime.datetime(2018, 1, 1, 11, 29, 30), 1],
    # valid token
    ['0ldc0ffe3', datetime.datetime(2018, 1, 1, 12, 30, 00), 0]
]


@pytest.mark.parametrize(args, params)
@pytest.mark.asyncio
async def test_set_valid_token(token, expiry, exp_mocked_refresh, client,
                               monkeypatch):
    """Refresh tokens if invalid or not set."""
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    client._auth_client.creds.token = token
    client._auth_client.creds.expiry = expiry

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    patch = 'gordon_gcp.clients.http.datetime.datetime'
    with mock.patch(patch, conftest.MockDatetime):
        await client.set_valid_token()

    assert exp_mocked_refresh == mock_refresh_token_called


#####
# Tests & fixtures for HTTP request handling
#####
@pytest.mark.asyncio
async def test_request(client, monkeypatch, caplog):
    """HTTP GET request is successful."""
    mock_set_valid_token_called = 0

    async def mock_set_valid_token():
        nonlocal mock_set_valid_token_called
        mock_set_valid_token_called += 1

    monkeypatch.setattr(client, 'set_valid_token', mock_set_valid_token)

    resp_text = 'ohai'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=200, body=resp_text)
        resp = await client.request('get', conftest.API_URL)

    assert resp == resp_text

    assert 1 == mock_set_valid_token_called
    request = mocked.requests[('get', conftest.API_URL)][0]
    authorization_header = request.kwargs['headers']['Authorization']
    assert authorization_header == f'Bearer {client._auth_client.token}'
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_request_refresh(client, monkeypatch, caplog):
    """HTTP GET request is successful while refreshing token."""
    mock_set_valid_token_called = 0

    async def mock_set_valid_token():
        nonlocal mock_set_valid_token_called
        mock_set_valid_token_called += 1

    monkeypatch.setattr(client, 'set_valid_token', mock_set_valid_token)

    resp_text = 'ohai'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=401)
        mocked.get(conftest.API_URL, status=200, body=resp_text)
        resp = await client.request('get', conftest.API_URL)

    assert resp == resp_text
    assert 2 == mock_set_valid_token_called
    assert 5 == len(caplog.records)


@pytest.mark.asyncio
async def test_request_max_refresh_reached(client, monkeypatch, caplog):
    """HTTP GET request is not successful from max refresh requests met."""
    mock_set_valid_token_called = 0

    async def mock_set_valid_token():
        nonlocal mock_set_valid_token_called
        mock_set_valid_token_called += 1

    monkeypatch.setattr(client, 'set_valid_token', mock_set_valid_token)

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=401)
        mocked.get(conftest.API_URL, status=401)
        mocked.get(conftest.API_URL, status=401)
        with pytest.raises(exceptions.GCPHTTPError) as e:
            await client.request('get', conftest.API_URL)

        e.match('Issue connecting to example.com:')

    assert 3 == mock_set_valid_token_called
    assert 9 == len(caplog.records)


def simple_json_callback(resp):
    raw_data = json.loads(resp)
    data = {}
    for key, value in raw_data.items():
        data["key"] = key
        data["value"] = value
    return data


args = 'json_func,exp_resp'
params = [
    [None, {'hello': 'world'}],
    [simple_json_callback, {'key': 'hello', 'value': 'world'}],
]


@pytest.mark.parametrize(args, params)
@pytest.mark.asyncio
async def test_get_json(json_func, exp_resp, client, monkeypatch, caplog):
    """HTTP GET request with JSON parsing."""
    mock_set_valid_token_called = 0

    async def mock_set_valid_token():
        nonlocal mock_set_valid_token_called
        mock_set_valid_token_called += 1

    monkeypatch.setattr(client, 'set_valid_token', mock_set_valid_token)

    resp_json = '{"hello": "world"}'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=200, body=resp_json)
        resp = await client.get_json(conftest.API_URL, json_func)

    assert exp_resp == resp
    assert 1 == mock_set_valid_token_called
    assert 2 == len(caplog.records)
