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

import datetime

import pytest
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins.service import event_consumer
from gordon_gcp.plugins.service import gdns_publisher


#####
# GDNS Publisher Fixtures
#####
@pytest.fixture
def config():
    return {
        'dns_zone': 'example.com.',
        'project': 'fakeproject',
        'api_version': 'v1',
        'default_ttl': 300,
        'publish_wait_timeout': 10
    }


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
def event_msg_data_with_no_resource_records(event_msg_data):
    event_msg_data['resourceRecords'] = []
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
def matching_zone_records():
    return [{
        'kind': 'dns#resourceRecordSet',
        'name': 'service.example.com.',
        'type': 'A',
        'ttl': 3600,
        'rrdatas': ['127.0.0.1']
    }]


@pytest.fixture
def event_message(mocker, event_msg_data):
    event_msg = mocker.MagicMock(event_consumer.GEventMessage)
    event_msg.msg_id = 'some-id-1234567890'
    event_msg.data = event_msg_data
    event_msg.phase = ''
    return event_msg


@pytest.fixture
def mock_dns_client(mocker, create_mock_coro, matching_zone_records,
                    initial_changes_resp):
    client = mocker.MagicMock()
    get_mock, get_coro = create_mock_coro()
    get_mock.return_value = matching_zone_records
    mocker.patch.object(client, 'get_records_for_zone', get_coro)
    mocker.patch.object(client, '_get_records_for_zone_mock', get_mock)

    post_mock, post_coro = create_mock_coro()
    post_mock.return_value = initial_changes_resp
    mocker.patch.object(client, 'publish_changes', post_coro)
    mocker.patch.object(client, '_publish_changes_mock', post_mock)

    done_mock, done_coro = create_mock_coro()
    done_mock.return_value = True
    mocker.patch.object(client, 'is_change_done', done_coro)
    mocker.patch.object(client, '_is_change_done_mock', done_mock)
    return client


@pytest.fixture
def gdns_publisher_instance(mocker, mock_dns_client, config, metrics):
    pb = gdns_publisher.GDNSPublisher(config, metrics, mock_dns_client)
    return pb


@pytest.fixture
def mock_sleep(mocker):
    sleep = mocker.Mock()

    async def mock_sleep(*args, **kwargs):
        sleep(*args, **kwargs)
    mocker.patch('asyncio.sleep', mock_sleep)
    return sleep


#####
# GDNS Publisher Tests
#####
def test_implements_interface(gdns_publisher_instance, metrics):
    """GDNSPublisher implements IMessageHandler"""

    assert interfaces.IMessageHandler.providedBy(gdns_publisher_instance)
    assert interfaces.IMessageHandler.implementedBy(
        gdns_publisher.GDNSPublisher)
    assert 'publish' == gdns_publisher_instance.phase


@pytest.mark.asyncio
async def test_handle_message_raises_exception_on_invalid_zone(
        gdns_publisher_instance, event_message,
        event_msg_data_with_invalid_zone, caplog):
    """Ensure exception raised on invalid zone"""
    event_message.data = event_msg_data_with_invalid_zone

    with pytest.raises(exceptions.InvalidDNSZoneInMessageError) as error:
        await gdns_publisher_instance.handle_message(
            event_message)
    assert error.match('Error when asserting zone for record:')
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_handle_message_handles_update_conflict(
        gdns_publisher_instance, event_message,
        initial_changes_req, handled_conflict_changes_req,
        initial_changes_pending_json_resp, initial_changes_resp, caplog,
        mocker, mock_dns_client):
    """Ensure changes with update conflicts are successfully published"""
    event_message.data['resourceRecords'] = initial_changes_req['additions']
    mock_dns_client._publish_changes_mock.side_effect = [
        exceptions.GCPHTTPResponseError('409', 409),
        initial_changes_pending_json_resp]

    await gdns_publisher_instance.handle_message(event_message)
    mock_dns_client._publish_changes_mock.assert_called_with(
        'example.com.', handled_conflict_changes_req)
    assert 3 == len(caplog.records)


@pytest.mark.asyncio
async def test_handle_message_raises_exception_on_publish_timeout(
        gdns_publisher_instance, event_message, mock_dns_client,
        initial_changes_req, mock_sleep, caplog, mocker):
    """Ensure exception raised when publish wait timeout exceeded."""
    start = datetime.datetime(2018, 1, 1, 11, 30, 0)
    mockdatetime = mocker.Mock()
    mockdatetime.now = mocker.Mock(side_effect=[
        start, start, start + datetime.timedelta(
            seconds=gdns_publisher_instance.config['publish_wait_timeout'] + 1)
    ])
    mocker.patch(
        'gordon_gcp.plugins.service.gdns_publisher.datetime.datetime',
        mockdatetime)
    event_message.data['resourceRecords'] = initial_changes_req['additions']
    mock_dns_client._is_change_done_mock.return_value = False

    with pytest.raises(exceptions.GCPPublishRecordTimeoutError) as e:
        await gdns_publisher_instance.handle_message(event_message)

    assert e.match('Timed out while waiting for DNS changes to transition '
                   'to \'done\' status.')

    mock_sleep.assert_called_with(1)
    assert 2 == len(caplog.records)


http_exceptions = [
    (('404', 404), exceptions.GCPHTTPResponseError),
    (('500', 500), exceptions.GCPHTTPResponseError),
    (('no_code',), exceptions.GCPHTTPError)
]


@pytest.mark.parametrize('exception_args,http_exception', http_exceptions)
@pytest.mark.asyncio
async def test_dispatch_changes_http_exceptions_raised(
        gdns_publisher_instance, resource_record,
        exception_args, http_exception):
    """Exception is raised when getting HTTP error from Google API."""

    gdns_publisher_instance.dns_client._publish_changes_mock.side_effect = \
        http_exception(*exception_args)

    with pytest.raises(http_exception):
        await gdns_publisher_instance._dispatch_changes(
            resource_record, None, None, None)


@pytest.mark.asyncio
async def test_handle_message_no_resource_records(
        gdns_publisher_instance, event_message,
        event_msg_data_with_no_resource_records, caplog):
    """Ensure message with no resource records is logged"""
    event_message.data = event_msg_data_with_no_resource_records

    await gdns_publisher_instance.handle_message(event_message)
    assert "Publisher received new message." in caplog.text
    assert ('No records published or deleted as no resource records were'
            ' present' in caplog.text)


@pytest.mark.asyncio
async def test_get_rrsets_by_name_and_type(
        gdns_publisher_instance, initial_changes_req,
        handled_conflict_changes_req):
    """Test correctly handling an additions conflict."""
    deletions = await gdns_publisher_instance._get_rrsets_by_name_and_type(
        'example.com.', initial_changes_req['additions'][0])

    assert handled_conflict_changes_req['deletions'] == deletions
