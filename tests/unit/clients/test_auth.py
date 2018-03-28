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
import os

import aiohttp
import pytest
from aioresponses import aioresponses
from google.oauth2 import _client as oauth_client
from google.oauth2 import service_account

from gordon_gcp import exceptions
from gordon_gcp.clients import auth


@pytest.fixture
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture
def mock_service_acct(mocker, monkeypatch):
    mock_creds = mocker.MagicMock(service_account.Credentials)
    sa_creds = mocker.MagicMock(service_account.Credentials)
    sa_creds._make_authorization_grant_assertion.return_value = 'deadb33f=='
    mock_creds.from_service_account_info.return_value = sa_creds

    patch = 'gordon_gcp.clients.auth.service_account.Credentials'
    monkeypatch.setattr(patch, mock_creds)
    return mock_creds


#####
# Tests for simple client instantiation
#####
args = 'scopes,provide_session,provide_loop'
params = [
    [['not-a-real-scope'], True, True],
    [['not-a-real-scope'], True, False],
    [['not-a-real-scope'], False, False],
    [['not-a-real-scope'], False, True],
    [None, True, True],
    [None, True, False],
    [None, False, False],
    [None, False, True],
]


@pytest.yield_fixture()
def custom_event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(args, params)
async def test_auth_client_default(scopes, provide_session, provide_loop,
                                   event_loop, fake_keyfile, fake_keyfile_data,
                                   mock_service_acct, custom_event_loop,
                                   session):
    """GAuthClient is created with expected attributes."""
    kwargs = {
        'keyfile': fake_keyfile,
        'scopes': scopes,
    }
    if provide_session:
        kwargs['session'] = session
    if provide_loop:
        kwargs['loop'] = custom_event_loop

    client = auth.GAuthClient(**kwargs)

    assert fake_keyfile_data == client._keydata

    if not scopes:
        scopes = ['cloud-platform']
    exp_scopes = [f'https://www.googleapis.com/auth/{s}' for s in scopes]

    assert exp_scopes == client.scopes
    assert isinstance(client._session, aiohttp.client.ClientSession)

    if provide_session:
        assert session is client._session
    else:
        assert session is not client._session
        # the session fixture cleans up the session, but when we don't
        # provide the session, it gets created for us, so we have to
        # clean up ourselves
        await client._session.close()

    if provide_loop and not provide_session:
        assert custom_event_loop is client._session.loop
    else:
        assert custom_event_loop is not client._session.loop

    assert not client.token
    assert not client.expiry


def test_auth_client_raises_json(tmpdir, caplog):
    """Client initialization raises when keyfile not valid json."""
    tmp_keyfile = tmpdir.mkdir('keys').join('broken_keyfile.json')
    tmp_keyfile.write('broken json')

    with pytest.raises(exceptions.GCPGordonError) as e:
        auth.GAuthClient(keyfile=tmp_keyfile)

    e.match(f'Keyfile {tmp_keyfile} is not valid JSON.')
    assert 1 == len(caplog.records)


def test_auth_client_raises_not_found(tmpdir, caplog):
    """Client initialization raises when keyfile not found."""
    tmp_keydir = tmpdir.mkdir('keys')
    no_keyfile = os.path.join(tmp_keydir, 'not-existent.json')

    with pytest.raises(exceptions.GCPGordonError) as e:
        auth.GAuthClient(keyfile=no_keyfile)

    e.match(f'Keyfile {no_keyfile} was not found.')
    assert 1 == len(caplog.records)


#####
# Tests & fixtures for access token handling
#####
@pytest.fixture
def mock_parse_expiry(mocker, monkeypatch):
    mock = mocker.MagicMock(oauth_client)
    mock._parse_expiry.return_value = datetime.datetime(2018, 1, 1, 12, 0, 0)
    monkeypatch.setattr('gordon_gcp.clients.auth._client', mock)
    return mock


@pytest.fixture
async def client(fake_keyfile, mock_service_acct, session, event_loop):
    yield auth.GAuthClient(keyfile=fake_keyfile, session=session)
    await session.close()


@pytest.mark.asyncio
async def test_refresh_token(client, fake_keyfile_data, mock_parse_expiry,
                             caplog):
    """Successfully refresh access token."""
    url = fake_keyfile_data['token_uri']
    token = 'c0ffe3'
    payload = {
        'access_token': token,
        'expires_in': 3600,  # seconds = 1hr
    }
    with aioresponses() as mocked:
        mocked.post(url, status=200, payload=payload)
        await client.refresh_token()
    assert token == client.token
    assert 2 == len(caplog.records)


args = 'status,payload,exc,err_msg'
params = [
    [504, None, exceptions.GCPHTTPError, 'Issue connecting to example.com'],
    [200, {}, exceptions.GCPAuthError, 'No access token in response.'],
]


@pytest.mark.parametrize(args, params)
@pytest.mark.asyncio
async def test_refresh_token_raises(status, payload, exc, err_msg, client,
                                    fake_keyfile_data, caplog):
    """Response errors from attempting to refresh token."""
    url = fake_keyfile_data['token_uri']

    with aioresponses() as mocked:
        mocked.post(url, status=status, payload=payload)
        with pytest.raises(exc) as e:
            await client.refresh_token()

        e.match(err_msg)

    assert 3 == len(caplog.records)
