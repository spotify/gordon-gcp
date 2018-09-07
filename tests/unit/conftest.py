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
from concurrent import futures

import aiohttp
import pytest
from google.cloud import pubsub

from gordon_gcp.clients import auth
from gordon_gcp.plugins.service import event_consumer


API_BASE_URL = 'https://example.com'
API_URL = f'{API_BASE_URL}/v1/foo_endpoint'


@pytest.fixture
def fake_response_data():
    return {
        'rrsets': [
            {
                'name': 'a-test.example.net.',
                'type': 'A',
                'ttl': 300,
                'rrdatas': [
                    '10.1.2.3',
                ],
                'kind': 'dns#resourceRecordSet'
            }, {
                'name': 'b-test.example.net.',
                'type': 'CNAME',
                'ttl': 600,
                'rrdatas': [
                    'a-test.example.net.',
                ],
                'kind': 'dns#resourceRecordSet'
            }, {
                'name': 'c-test.example.net.',
                'type': 'TXT',
                'ttl': 300,
                'rrdatas': [
                    '"OHAI"',
                    '"OYE"',
                ],
                'kind': 'dns#resourceRecordSet'
            }
        ]
    }


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
def config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'project': 'test-example',
        'topic': 'a-topic'
    }


@pytest.fixture
async def auth_client(mocker, monkeypatch):
    mock = mocker.Mock(auth.GAuthClient, autospec=True)
    mock.token = '0ldc0ffe3'
    mock._session = aiohttp.ClientSession()
    mock.refresh_token = mocker.Mock(auth.GAuthClient.refresh_token)
    mock.refresh_token.side_effect = _noop
    creds = mocker.Mock()
    mock.creds = creds
    monkeypatch.setattr('gordon_gcp.clients.auth.GAuthClient', mock)
    monkeypatch.setattr(
       'gordon_gcp.plugins.janitor.gpubsub_publisher.auth.GAuthClient', mock)
    yield mock
    await mock._session.close()


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    return caplog


@pytest.fixture
def create_mock_coro(mocker):
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
def creation_audit_log_data():
    resource_name = ('projects/123456789101/zones/us-central1-c/instances/'
                     'an-instance-name-b34c')
    return {
        'action': 'additions',
        'resourceName': resource_name,
        'timestamp': '2017-12-04T20:13:51.414016721Z'
    }


@pytest.fixture
def additions_gevent_msg(mocker, creation_audit_log_data):
    fake_data = {
            'action': creation_audit_log_data['action'],
            'resourceName': creation_audit_log_data['resourceName'],
            'resourceRecords': [],
            'timestamp': creation_audit_log_data['timestamp']
        }
    pubsub_msg = mocker.Mock()
    pubsub_msg._ack_id = '1:1'
    return event_consumer.GEventMessage(
        pubsub_msg, fake_data)


@pytest.fixture(scope='session')
def deletion_audit_log_data():
    resource_name = ('projects/123456789101/zones/us-central1-c/instances/'
                     'an-instance-name-abc1')
    return {
        'action': 'deletions',
        'resourceName': resource_name,
        'timestamp': '2017-12-04T20:13:51.414016722Z'
    }


@pytest.fixture
def deletions_gevent_msg(mocker, deletion_audit_log_data):
    fake_data = {
            'action': deletion_audit_log_data['action'],
            'resourceName': deletion_audit_log_data['resourceName'],
            'resourceRecords': [],
            'timestamp': deletion_audit_log_data['timestamp']
        }
    pubsub_msg = mocker.Mock()
    pubsub_msg._ack_id = '1:2'
    return event_consumer.GEventMessage(
        pubsub_msg, fake_data)


@pytest.fixture
def channel_pair():
    return asyncio.Queue(), asyncio.Queue()


async def _noop():
    pass


@pytest.fixture
def get_gce_client(mocker, auth_client):
    def _create_client(klass, *args, **kwargs):
        client = klass(auth_client, *args, **kwargs)
        mocker.patch.object(client, 'valid_token_set', _noop)
        return client
    return _create_client


@pytest.fixture
def mock_pubsub_client(mocker, monkeypatch):
    mock = mocker.Mock(pubsub.PublisherClient, autospec=True)
    patch = (
        'gordon_gcp.plugins.janitor.gpubsub_publisher.pubsub.PublisherClient')
    monkeypatch.setattr(patch, mock)
    future = futures.Future()
    future.set_result('id123456790')
    mock.publish.return_value = future
    return mock


@pytest.fixture
def authority_config():
    return {
        'keyfile': 'keyfile',
        'scopes': ['scope'],
        'metadata_blacklist': [['key', 'val'], ['other_key', 'other_val']],
        'project_blacklist': [],
        'tag_blacklist': [],
        'dns_zone': 'zone1.com',
    }


@pytest.fixture
def instance_data():
    return {
        'id': '1',
        'creationTimestamp': '2018-01-01 00:00:00.0000',
        'name': 'instance-1',
        'description': 'guc3-instance-1-54kj',
        'tags': {
            'items': ['some-tag'],
            'fingerprint': ''
        },
        'machineType': 'n1-standard-1',
        'status': 'RUNNING',
        'statusMessage': 'RUNNING',
        'zone': 'us-west9-z',
        'canIpForward': False,
        'networkInterfaces': [{
            'network': 'network/url/string',
            'subnetwork': 'subnetwork/url/string',
            'networkIP': '192.168.0.1',
            'name': 'test-network',
            'accessConfigs': [{
                'type': 'ONE_TO_ONE_NAT',
                'name': 'EXTERNAL NAT',
                'natIP': '1.1.1.1',
                'kind': 'compute#accessConfig'
            }],
        }],
        'metadata': {
            'items': [
                {'key': 'default', 'value': 'true'}
            ],
        },
    }


@pytest.fixture
def emulator(monkeypatch):
    monkeypatch.delenv('PUBSUB_EMULATOR_HOST', raising=False)


@pytest.fixture
def metrics(mocker):
    return MetricRelayStub(mocker, config={})


class MetricRelayStub:
    def __init__(self, mocker, config):
        self.config = config
        self._incr_mock = mocker.Mock()
        self._set_mock = mocker.Mock()
        self._timer_mock = mocker.Mock()
        self.timer_stub = None
        self._mocker = mocker

    async def incr(self, metric_name, value=1, context=None, **kwargs):
        self._incr_mock(metric_name, value=value, context=context, **kwargs)

    def timer(self, metric_name, context=None, **kwargs):
        self._timer_mock(metric_name, context=context, **kwargs)
        self.timer_stub = TimerStub(self._mocker)
        return self.timer_stub

    async def set(self, metric_name, value, context=None, **kwargs):
        self._set_mock(metric_name, value, context=context, **kwargs)


class TimerStub:
    def __init__(self, mocker):
        self.start_mock = mocker.Mock()
        self.stop_mock = mocker.Mock()

    async def start(self, *args, **kwargs):
        self.start_mock(*args, **kwargs)

    async def stop(self, *args, **kwargs):
        self.stop_mock(*args, **kwargs)
