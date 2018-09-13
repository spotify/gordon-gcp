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


args = 'token,expiry,is_valid_token_expected'
params = [
    # no token - expiry doesn't matter
    [None, datetime.datetime(2018, 1, 1, 9, 30, 0), False],
    # expired token
    ['0ldc0ffe3', datetime.datetime(2018, 1, 1, 9, 30, 0), False],
    # token expires within 60 seconds
    ['0ldc0ffe3', datetime.datetime(2018, 1, 1, 11, 29, 30), False],
    # valid token
    ['0ldc0ffe3', datetime.datetime(2018, 1, 1, 12, 30, 00), True]
]


@pytest.mark.parametrize(args, params)
@pytest.mark.asyncio
async def test_valid_token_set(token, expiry, is_valid_token_expected, client,
                               monkeypatch):
    """Refresh tokens if invalid or not set."""
    client._auth_client.token = token
    client._auth_client.expiry = expiry

    patch = 'gordon_gcp.clients.http.datetime.datetime'
    with mock.patch(patch, conftest.MockDatetime):
        is_valid_token = await client.valid_token_set()

    assert is_valid_token == is_valid_token_expected


#####
# Tests & fixtures for HTTP request handling
#####
@pytest.mark.asyncio
async def test_request(client, monkeypatch, caplog):
    """HTTP GET request is successful."""
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    async def mock_valid_token_set():
        return False

    monkeypatch.setattr(client, 'valid_token_set', mock_valid_token_set)

    resp_text = 'ohai'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=200, body=resp_text)
        resp = await client.request('get', conftest.API_URL)

    assert resp == resp_text

    assert 1 == mock_refresh_token_called
    request = mocked.requests[('get', conftest.API_URL)][0]
    authorization_header = request.kwargs['headers']['Authorization']
    assert authorization_header == f'Bearer {client._auth_client.token}'
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_request_refresh(client, monkeypatch, caplog):
    """HTTP GET request is successful while refreshing token."""
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    async def mock_valid_token_set():
        pass

    monkeypatch.setattr(client, 'valid_token_set', mock_valid_token_set)

    resp_text = 'ohai'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=401)
        mocked.get(conftest.API_URL, status=200, body=resp_text)
        resp = await client.request('get', conftest.API_URL)

    assert 2 == mock_refresh_token_called
    assert resp == resp_text
    assert 6 == len(caplog.records)


@pytest.mark.asyncio
async def test_request_max_request_attempts_reached(
        client, monkeypatch, caplog):
    """HTTP GET request is not successful if max requests amount met."""
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    async def mock_valid_token_set():
        pass

    monkeypatch.setattr(client, 'valid_token_set', mock_valid_token_set)

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=401)
        mocked.get(conftest.API_URL, status=401)
        mocked.get(conftest.API_URL, status=401)
        with pytest.raises(exceptions.GCPHTTPResponseError) as e:
            await client.request('get', conftest.API_URL)

        e.match('HTTP error response from https://example.com/v1/foo_endpoint:')

    assert 2 == mock_refresh_token_called
    assert 9 == len(caplog.records)


@pytest.mark.asyncio
async def test_request_with_zero_token_refresh_attempts(
        client, monkeypatch, caplog):
    """HTTP GET request is successful with no token refresh attempts."""
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    async def mock_valid_token_set():
        pass

    monkeypatch.setattr(client, 'valid_token_set', mock_valid_token_set)

    resp_text = 'ohai'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=200, body=resp_text)
        resp = await client.request(
            'get', conftest.API_URL, token_refresh_attempts=0)

    assert 0 == mock_refresh_token_called
    assert resp == resp_text
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_request_with_valid_token(
        client, monkeypatch, caplog):
    """HTTP GET request is successful with a valid token."""
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    async def mock_valid_token_set():
        return True

    monkeypatch.setattr(client, 'valid_token_set', mock_valid_token_set)

    resp_text = 'ohai'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=200, body=resp_text)
        resp = await client.request(
            'get', conftest.API_URL, token_refresh_attempts=1)

    assert 0 == mock_refresh_token_called
    assert resp == resp_text
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_request_with_other_exception(client, mocker, monkeypatch,
                                            caplog):
    """Correct exception is raised when the aiohttp request itself fails."""

    async def mock_coro():
        pass

    monkeypatch.setattr(client._auth_client, 'refresh_token', mock_coro)
    monkeypatch.setattr(client, 'valid_token_set', mock_coro)

    monkeypatch.setattr('uuid.uuid4', lambda: 'FOO')

    class AsyncContextManager:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            raise ValueError('BAR')  # randomly chosen

        async def __aexit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr(aiohttp.ClientSession, 'request', AsyncContextManager)

    exp_msg = r'\[FOO\] Request call failed: BAR'
    with pytest.raises(exceptions.GCPHTTPError, match=exp_msg):
        await client.request(
            'get', conftest.API_URL, token_refresh_attempts=0)


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
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    async def mock_valid_token_set():
        pass

    monkeypatch.setattr(client, 'valid_token_set', mock_valid_token_set)

    resp_json = '{"hello": "world"}'

    with aioresponses() as mocked:
        mocked.get(conftest.API_URL, status=200, body=resp_json)
        resp = await client.get_json(conftest.API_URL, json_func)

    assert exp_resp == resp
    assert 1 == mock_refresh_token_called
    assert 2 == len(caplog.records)


@pytest.mark.parametrize('post_arg', ('json', 'data'))
@pytest.mark.asyncio
async def test_post_data(post_arg, client, monkeypatch, caplog):
    """HTTP POST request."""
    mock_refresh_token_called = 0

    async def mock_refresh_token():
        nonlocal mock_refresh_token_called
        mock_refresh_token_called += 1

    monkeypatch.setattr(
        client._auth_client, 'refresh_token', mock_refresh_token)

    async def mock_valid_token_set():
        pass

    monkeypatch.setattr(client, 'valid_token_set', mock_valid_token_set)

    post_data = {'hello': 'world'}

    request_kwargs = {
        'method': 'post',
        'url': conftest.API_URL,
        post_arg: post_data
    }

    with aioresponses() as mocked:
        mocked.post(conftest.API_URL, status=204)
        resp = await client.request(**request_kwargs)

    assert '' == resp
    assert 1 == mock_refresh_token_called
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_post_json_raises(client, monkeypatch, caplog):
    """POST JSON data and json fails."""
    post_json = {'hello': 'world'}
    exp_msg = ('"data" and "json" request parameters can not be used '
               'at the same time')
    with pytest.raises(exceptions.GCPHTTPError, match=exp_msg):
        await client.request(
            'post', conftest.API_URL, data=post_json, json=post_json)

    assert 1 == len(caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize('max_pages,test_param', [
        [1, None],
        [2, {'test': 'param'}]
    ]
    )
async def test_get_all(mocker, max_pages, test_param):
    number_of_calls = 0

    class TestClient(http.AIOConnection):
        async def get_json(self, url, params=None):
            if test_param is None:
                assert params == {}
            nonlocal number_of_calls
            if number_of_calls < (max_pages - 1):
                number_of_calls += 1
                return {'data': 'data', 'nextPageToken': 'token'}
            # last page doesn't include a nextPageToken
            return {'data': 'final page'}

    auth_client = mocker.Mock(auth.GAuthClient)
    auth_client._session = aiohttp.ClientSession()
    simple_paging_client = TestClient(auth_client)

    results = await simple_paging_client.get_all(
        conftest.API_BASE_URL, params=test_param)
    assert max_pages == len(results)
