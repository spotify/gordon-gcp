# -*- coding: utf-8 -*-
#
# Copyright 2017 Spotify AB
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
import json

import pytest
from google.cloud import pubsub_v1
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import http
from gordon_gcp.plugins import event_consumer, publisher


#####
# Publisher Fixtures
#####
@pytest.fixture
def config():
    return {
        'managed_zone': 'example-com',
        'dns_zone': 'example.com.',
        'project': 'fakeproject',
        'api_version': 'v1',
        'default_ttl': 300,
        'publish_wait_timeout': 10
    }


@pytest.fixture
def rrsets_url(config):
    project = config['project']
    managed_zone = config['managed_zone']
    return (f'https://www.googleapis.com/dns/v1/projects/{project}/'
            f'managedZones/{managed_zone}/rrsets')


@pytest.fixture
def changes_url(config):
    project = config['project']
    managed_zone = config['managed_zone']
    return (f'https://www.googleapis.com/dns/v1/projects/{project}/'
            f'managedZones/{managed_zone}/changes')


@pytest.fixture
def resource_record():
    return {
        'name': 'service.example.com.',
        'rrdatas': ['127.0.0.1'],
        'type': 'A',
        'ttl': 3600
    }


@pytest.fixture
def event_msg_data(resource_record):
    return {
        'action': 'additions',
        'resourceName':
            'projects/a-project-id/zones/a-zone-name/instances/an-instance',
        'resourceRecords': [
            resource_record,
            {
                'name': 'subservice.example.com.',
                'rrdatas': ['127.0.0.2'],
                'type': 'A',
                'ttl': 3600
            }
        ]
    }


@pytest.fixture
def event_msg_data_with_invalid_zone(event_msg_data):
    event_msg_data['resourceRecords'][0]['name'] = 'brokenexample.com.'
    return event_msg_data


@pytest.fixture
def initial_changes_req():
    return {
        'kind': 'dns#change',
        'additions': [
            {
                'kind': 'dns#resourceRecordSet',
                'name': 'service.example.com.',
                'type': 'A',
                'ttl': 3600,
                'rrdatas': ['127.0.0.1', '127.0.0.2']
            }
        ]
    }


@pytest.fixture
def initial_changes_pending_json_resp():
    return """{
        "kind": "dns#change",
        "additions": [{
            "kind": "dns#resourceRecordSet",
            "name": "service.example.com.",
            "rrdatas": ["127.0.0.1", "127.0.0.2"],
            "type": "A",
            "ttl": 3600
        }],
        "startTime": "2018-04-26T15:02:17.541Z",
        "id": "999",
        "status": "pending"
    }"""


@pytest.fixture
def initial_changes_resp():
    return {
        'kind': 'dns#change',
        "additions": [{
            "kind": "dns#resourceRecordSet",
            "name": "service.example.com.",
            "rrdatas": ["127.0.0.1", "127.0.0.2"],
            "type": "A",
            "ttl": 3600
        }],
        'startTime': '2018-04-26T15:02:17.541Z',
        'id': '999',
        'status': 'done'
    }


@pytest.fixture
def handled_conflict_changes_req():
    return {
        'kind': 'dns#change',
        'additions': [
            {
                'kind': 'dns#resourceRecordSet',
                'name': 'service.example.com.',
                'type': 'A',
                'ttl': 3600,
                'rrdatas': ['127.0.0.1', '127.0.0.2']
            }
        ],
        'deletions': [
            {
                'kind': 'dns#resourceRecordSet',
                'name': 'service.example.com.',
                'type': 'A',
                'ttl': 3600,
                'rrdatas': ['127.0.0.1']
            }
        ],

    }


@pytest.fixture
def handled_conflict_changes_done():
    return {
        'kind': 'dns#change',
        'additions': [
            {
                'kind': 'dns#resourceRecordSet',
                'name': 'service.example.com.',
                'type': 'A',
                'ttl': 3600,
                'rrdatas': ['127.0.0.1', '127.0.0.2']
            }
        ],
        'deletions': [
            {
                'kind': 'dns#resourceRecordSet',
                'name': 'service.example.com.',
                'type': 'A',
                'ttl': 3600,
                'rrdatas': ['127.0.0.1']
            }
        ],
        'startTime': '2018-05-01T18:41:51.577Z',
        'id': '13',
        'status': 'done'
    }


@pytest.fixture
def all_existing_zone_records():
    return {
        'kind': 'dns#resourceRecordSetsListResponse',
        'rrsets': [
            {
                'kind': 'dns#resourceRecordSet',
                'name': 'example.com.',
                'type': 'SOA',
                'ttl': 21600,
                'rrdatas': [
                    'ns-cloud-c1.googledomains.com.'
                    ' cloud-dns-hostmaster.google.com. '
                    '1 21600 3600 259200 300'
                ]
            },
            {
                'kind': 'dns#resourceRecordSet',
                'name': 'service.example.com.',
                'type': 'A',
                'ttl': 3600,
                'rrdatas': ['127.0.0.1']
            }
        ]
    }


@pytest.fixture
def all_existing_zone_records_empty():
    return {
        'kind': 'dns#resourceRecordSetsListResponse',
        'rrsets': []
    }


@pytest.fixture
def pubsub_message(mocker):
    pubsub_msg = mocker.MagicMock(pubsub_v1.subscriber.message.Message)
    pubsub_msg.message_id = 1234
    return pubsub_msg


@pytest.fixture
def event_message(mocker, event_msg_data, pubsub_message):
    event_msg = mocker.MagicMock(event_consumer.GEventMessage)
    event_msg.msg_id = pubsub_message.message_id
    event_msg.data = event_msg_data
    event_msg.phase = ''
    return event_msg


@pytest.fixture
def mock_http_client(mocker, get_mock_coro):
    get_all_mock, get_all_coro = get_mock_coro()
    get_json_mock, get_json_coro = get_mock_coro()
    request_post_mock, post_request_coro = get_mock_coro()

    http_client = mocker.MagicMock()

    mocker.patch.object(http_client, 'get_all', get_all_coro)
    mocker.patch.object(http_client, '_get_all_mock', get_all_mock)

    mocker.patch.object(http_client, 'get_json', get_json_coro)
    mocker.patch.object(http_client, '_get_json_mock', get_json_mock)

    mocker.patch.object(http_client, 'request', post_request_coro)
    mocker.patch.object(http_client, '_request_post_mock', request_post_mock)

    return http_client


@pytest.fixture
def publisher_instance(mock_http_client, config):
    success, error = asyncio.Queue(), asyncio.Queue()
    pb = publisher.GDNSPublisher(config, success, error, mock_http_client)
    return pb


@pytest.fixture
def mock_sleep(mocker):
    sleep = mocker.Mock()

    async def mock_sleep(*args, **kwargs):
        sleep(*args, **kwargs)
    mocker.patch('asyncio.sleep', mock_sleep)
    return sleep


#####
# Publisher Tests
#####
def test_implements_interface(config, auth_client):
    """GDNSPublisher implements IPublisherClient"""
    client = http.AIOConnection(auth_client=auth_client)

    success, error = asyncio.Queue(), asyncio.Queue()
    plugin = publisher.GDNSPublisher(config, success, error, client)

    assert interfaces.IPublisherClient.providedBy(plugin)
    assert interfaces.IPublisherClient.implementedBy(publisher.GDNSPublisher)
    assert config is plugin.config
    assert success is plugin.success_channel
    assert error is plugin.error_channel
    assert 'publish' == plugin.phase


@pytest.mark.asyncio
async def test_publish_changes_raises_exception_on_invalid_zone(
        publisher_instance, event_message, event_msg_data_with_invalid_zone,
        caplog):
    """Ensure exception raised on invalid zone"""
    event_message.data = event_msg_data_with_invalid_zone

    with pytest.raises(exceptions.InvalidDNSZoneInMessageError) as error:
        await publisher_instance.publish_changes(
            event_message)
    assert error.match('Error when asserting zone for record:')
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_publish_changes_handles_update_conflict(
    publisher_instance, event_message,
        all_existing_zone_records, rrsets_url, changes_url,
        initial_changes_req, handled_conflict_changes_req,
        initial_changes_pending_json_resp, initial_changes_resp, caplog):
    """Ensure exception raised on invalid zone"""
    expected_change_id = json.loads(initial_changes_pending_json_resp)['id']
    event_message.data['resourceRecords'] = initial_changes_req['additions']
    publisher_instance.http_client._request_post_mock.side_effect = [
        exceptions.GCPHTTPError('409'), initial_changes_pending_json_resp]
    publisher_instance.http_client._get_all_mock.return_value = \
        all_existing_zone_records
    publisher_instance.http_client._get_json_mock.return_value = \
        initial_changes_resp

    await publisher_instance.publish_changes(event_message)

    publisher_instance.http_client._get_all_mock.assert_called_with(rrsets_url)
    publisher_instance.http_client._request_post_mock.assert_called_with(
        'post', changes_url, json=handled_conflict_changes_req)
    publisher_instance.http_client._get_json_mock.assert_called_with(
        f'{changes_url}/{expected_change_id}')
    assert 3 == len(caplog.records)


@pytest.mark.asyncio
async def test_publish_changes_raises_exception_on_publish_timeout(
    publisher_instance, event_message, changes_url,
        initial_changes_req, initial_changes_resp,
        initial_changes_pending_json_resp, mock_sleep, caplog):
    """Ensure exception raised when publish wait timeout exceeded."""
    publisher_instance.publish_wait_timeout = 0.0001
    event_message.data['resourceRecords'] = initial_changes_req['additions']
    publisher_instance.http_client._request_post_mock.return_value = \
        initial_changes_pending_json_resp
    initial_changes_resp['status'] = 'pending'
    publisher_instance.http_client._get_json_mock.return_value = \
        initial_changes_resp

    with pytest.raises(exceptions.GCPPublishRecordTimeoutError) as error:
        await publisher_instance.publish_changes(event_message)

    assert error.match('Timed out while waiting for DNS changes to transition '
                       'to \'done\' status.')
    expected_change_id = json.loads(initial_changes_pending_json_resp)['id']
    publisher_instance.http_client._get_json_mock.assert_called_with(
        f'{changes_url}/{expected_change_id}')
    mock_sleep.assert_called_with(1)
    assert 2 == len(caplog.records)


http_exceptions = [
    ('404', exceptions.GCPHTTPNotFoundError),
    ('409', exceptions.GCPHTTPConflictError),
    ('500', exceptions.GCPHTTPError),
    ('no_code', exceptions.GCPHTTPError)
]


@pytest.mark.parametrize('status_code,http_exception', http_exceptions)
@pytest.mark.asyncio
async def test__publish_changes_http_exceptions_raised(
        publisher_instance, initial_changes_req, status_code,
        http_exception):
    """Exception is raised when getting HTTP error from Google API."""

    publisher_instance.http_client._request_post_mock.side_effect = \
        exceptions.GCPHTTPError(status_code)

    with pytest.raises(http_exception):
        await publisher_instance._publish_changes(initial_changes_req)


@pytest.mark.asyncio
async def test__publish_changes_returns_change_id(
        publisher_instance, initial_changes_req,
        initial_changes_pending_json_resp):
    """Ensure change ID is returned from Google API."""
    expected_change_id = json.loads(initial_changes_pending_json_resp)['id']
    publisher_instance.http_client._request_post_mock.return_value = (
        initial_changes_pending_json_resp)

    change_id = await publisher_instance._publish_changes(
        initial_changes_req)

    assert expected_change_id == change_id


conflict_types = [
    (all_existing_zone_records(), handled_conflict_changes_req()),
    (all_existing_zone_records_empty(), initial_changes_req())
]


@pytest.mark.parametrize('existing,output', conflict_types)
@pytest.mark.asyncio
async def test__handle_additions_conflict(
        publisher_instance, existing, initial_changes_req, output):
    """Test correctly handling an additions conflict."""
    publisher_instance.http_client._get_all_mock.return_value = existing

    changes = await publisher_instance._handle_additions_conflict(
        initial_changes_req)

    assert changes == output
