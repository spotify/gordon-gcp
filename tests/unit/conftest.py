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
"""
Module for reusable pytest fixtures.
"""

import asyncio
import datetime
import json
import logging

import aiohttp
import pytest

from gordon_gcp.clients import auth
from gordon_gcp.plugins import event_consumer


API_BASE_URL = 'https://example.com'
API_URL = f'{API_BASE_URL}/v1/foo_endpoint'


@pytest.fixture
def fake_keyfile_data():
    return {
        'type': 'service_account',
        'project_id': 'a-test-project',
        'private_key_id': 'yeahright',
        'private_key': 'nope',
        'client_email': 'test-key@a-test-project.iam.gserviceaccount.com',
        'client_id': '12345678910',
        'auth_uri': f'{API_BASE_URL}/auth',
        'token_uri': f'{API_BASE_URL}/token',
        'auth_provider_x509_cert_url': f'{API_BASE_URL}/certs',
        'client_x509_cert_url': f'{API_BASE_URL}/x509/a-test-project'
    }


@pytest.fixture
def fake_keyfile(fake_keyfile_data, tmpdir):
    tmp_keyfile = tmpdir.mkdir('keys').join('fake_keyfile.json')
    tmp_keyfile.write(json.dumps(fake_keyfile_data))
    return tmp_keyfile


# pytest prevents monkeypatching datetime directly
class MockDatetime(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return datetime.datetime(2018, 1, 1, 11, 30, 0)


@pytest.fixture
async def auth_client(mocker, monkeypatch):
    mock = mocker.Mock(auth.GAuthClient)
    mock.token = '0ldc0ffe3'
    mock._session = aiohttp.ClientSession()
    creds = mocker.Mock()
    mock.creds = creds
    monkeypatch.setattr('gordon_gcp.clients.auth.GAuthClient', mock)
    yield mock
    await mock._session.close()


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    return caplog


@pytest.fixture
def get_mock_coro(mocker):
    """Create a mock-coro pair.

    The coro can be used to patch an async method while the mock can
    be used to assert calls to the mocked out method.
    """
    def _create_mock_coro_pair():
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)
        return mock, _coro
    return _create_mock_coro_pair


@pytest.fixture(scope='session')
def audit_log_data():
    resource_name = ('projects/123456789101/zones/us-central1-c/instances/'
                     'an-instance-name-b34c')
    return {
        'action': 'additions',
        'resourceName': resource_name,
        'timestamp': '2017-12-04T20:13:51.414016721Z'
    }


@pytest.fixture
def gevent_msg(mocker, audit_log_data):
    fake_data = {
            'action': audit_log_data['action'],
            'resourceName': audit_log_data['resourceName'],
            'resourceRecords': [],
            'timestamp': audit_log_data['timestamp']
        }
    pubsub_msg = mocker.Mock()
    pubsub_msg._ack_id = '1:1'
    return event_consumer.GEventMessage(
        pubsub_msg, fake_data)


@pytest.fixture
def channel_pair():
    return asyncio.Queue(), asyncio.Queue()
