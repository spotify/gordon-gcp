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
import pytest
from google.cloud import pubsub_v1
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins import publisher
from gordon_gcp.clients import http
from gordon_gcp.plugins import event_consumer


@pytest.fixture
def config():
    return {'timeout': 90,
            'managed_zone': 'nurit-com',
            'project': 'pr-tower-hackweek',
            'api_version': 'v1',
            'dns_zone': 'nurit.com.'
    }


@pytest.fixture
def pubsub_message(mocker):
    pubsub_msg = mocker.MagicMock(pubsub_v1.subscriber.message.Message)
    pubsub_msg.message_id = 1234
    return pubsub_msg


@pytest.fixture
def mock_http_client(mocker, get_mock_coro):
    get_json_mock, get_json_coro = get_mock_coro()
    request_post_mock, post_request_coro = get_mock_coro()

    http_client = mocker.MagicMock()

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
def event_msg_data():
    return {
        'action': 'additions',
        'resourceName': 'projects/.../instances/an-instance-name-b45c',
        'resourceRecords': [
            {
                'name': 'service3.nurit.com.',
                'rrdatas': ['127.10.20.2'],
                'type': 'A',
                'ttl': 3600
            },
            {
                'name': 'service4.nurit.com.',
                'rrdatas': ['127.10.20.5'],
                'type': 'A',
                'ttl': 3600
            }

        ]
    }


@pytest.fixture
def event_msg_data_with_invalid_zone():
    return {
        'action': 'additions',
        'resourceName': 'projects/.../instances/an-instance-name-b45c',
        'resourceRecords': [
            {
                'name': 'service.example.com.',
                'rrdatas': ['1.1.1.1'],
                'type': 'A',
                'ttl': 3600
            }
        ]
    }


@pytest.fixture
def event_msg_data_with_invalid_action():
    return {
        'action': 'updating',
        'resourceName': 'projects/.../instances/an-instance-name-b45c',
        'resourceRecords': [
            {
                'name': 'service.nurit.com.',
                'rrdatas': ['1.1.1.1'],
                'type': 'A',
                'ttl': 3600
            }
        ]
    }


@pytest.fixture
def event_msg_data_delete_unexisting_record():
    return {
        'action': 'deletions',
        'resourceName': 'projects/.../instances/an-instance-name-b45c',
        'resourceRecords': [
            {
                'name': 'service5.nurit.com.',
                'rrdatas': ['127.10.20.8'],
                'type': 'A',
                'ttl': 3600
            }
        ]
    }


@pytest.fixture
def event_msg_data_bad_rrdata():
    return {
        'action': 'additions',
        'resourceName': 'projects/.../instances/an-instance-name-b45c',
        'resourceRecords': [
            {
                'name': 'service5.nurit.com.',
                'rrdatas': ['127.10.20.899'],
                'type': 'A',
                'ttl': 3600
            }
        ]
    }


@pytest.fixture
def event_message(mocker, event_msg_data, pubsub_message):
    event_msg = mocker.MagicMock(event_consumer.GEventMessage)
    event_msg.msg_id = pubsub_message.message_id
    event_msg.data = event_msg_data
    event_msg.phase = ''
    return event_msg


@pytest.fixture
def event_message_delete_unexisting_record(
        event_message,
        event_msg_data_delete_unexisting_record):
    event_message.data = event_msg_data_delete_unexisting_record
    return event_message


@pytest.fixture
def api_response_on_request_changes():
    return """{
                     "kind": "dns#change",
                     "additions": [
                      {
                       "kind": "dns#resourceRecordSet",
                       "name": "service3.nurit.com.",
                       "type": "A",
                       "ttl": 3600,
                       "rrdatas": [
                        "127.10.20.2"
                       ]
                      }
                     ],
                     "startTime": "2018-04-26T15:22:36.941Z",
                     "id": "6",
                     "status": "pending"
                    }"""


@pytest.fixture
def api_response_json_on_get_json():
    return {'kind': 'dns#change',
            'additions': [{'kind': 'dns#resourceRecordSet',
                           'name': 'service3.nurit.com.',
                           'type': 'A',
                           'ttl': 3600,
                           'rrdatas': ['127.10.20.2']}],
            'startTime': '2018-04-26T15:02:17.541Z',
            'id': '4',
            'status': 'done'}


def test_implements_interface(config, auth_client):
    """GDNSPublisher implements IPublisherClient"""
    client = http.AIOConnection(auth_client=auth_client)

    success, error = asyncio.Queue(), asyncio.Queue()
    client = publisher.GDNSPublisher(config, success, error, client)

    assert interfaces.IPublisherClient.providedBy(client)
    assert interfaces.IPublisherClient.implementedBy(publisher.GDNSPublisher)
    assert config is client.config
    assert success is client.success_channel
    assert error is client.error_channel
    assert 'publish' == client.phase


@pytest.mark.asyncio
async def test_find_zone_failed(
        publisher_instance, event_message,
        event_msg_data_with_invalid_zone, caplog):
    """Test error is raised a valid zone is not found in the record"""
    event_message.data = event_msg_data_with_invalid_zone

    record = event_message.data['resourceRecords'][0]

    await publisher_instance.publish_changes(event_message)

    expected_msg = f'[msg-1234]: DROPPING: Fatal exception ' \
                   f'occurred when handling message: ' \
                   f'Error when asserting zone' \
                   f' for record: {record}.'

    actual_msg = caplog.records[1].msg

    assert expected_msg == actual_msg


@pytest.mark.asyncio
async def test_invalid_action(
        publisher_instance, event_message,
        event_msg_data_with_invalid_action, caplog):
    """Test error is raised when the action is invalid in the record"""
    event_message.data = event_msg_data_with_invalid_action

    action = event_message.data['action']

    await publisher_instance.publish_changes(event_message)

    expected_msg = f'[msg-1234]: DROPPING: Fatal exception ' \
                   f'occurred when handling message: ' \
                   f'Error trying to format changes, ' \
                   f'got an invalid action: {action}.'

    actual_msg = caplog.records[1].msg

    assert expected_msg == actual_msg


@pytest.mark.asyncio
async def test_failed_on_post_adding_existing_record(
        publisher_instance, event_message, caplog):
    """Test error is raised and the message is dropped
        when trying to add an existing record"""
    error = "Issue connecting to www.googleapis.com: 409, message='Conflict'"
    changes = {'kind': 'dns#change',
               'additions':
                   [{'kind': 'dns#resourceRecordSet',
                     'name': 'service3.nurit.com.',
                     'type': 'A', 'ttl': 3600,
                     'rrdatas': ['127.10.20.2']}]
    }

    publisher_instance.http_client._request_post_mock.side_effect =\
        exceptions.GCPHTTPError(error)

    await publisher_instance.publish_changes(event_message)

    actual_msg = caplog.records[1].msg
    expected_msg = f'[msg-1234]: DROPPING: Fatal exception ' \
                   f'occurred when handling message: ' \
                   f'Error: {error} for changes: {changes}.'
    assert expected_msg == actual_msg


@pytest.mark.asyncio
async def test_failed_on_post_delete_unexisting_record(
        publisher_instance,
        event_message_delete_unexisting_record, caplog):
    """Test error is raised and the message is dropped
        when trying to delete un existing record"""
    error = "Issue connecting to www.googleapis.com: 404, message='Not Found'"
    changes = {'kind': 'dns#change',
               'deletions':
                   [{'kind': 'dns#resourceRecordSet',
                     'name': 'service5.nurit.com.',
                     'type': 'A', 'ttl': 3600,
                     'rrdatas': ['127.10.20.8']}]
               }
    publisher_instance.http_client._request_post_mock.side_effect = \
        exceptions.GCPHTTPError(error)

    await publisher_instance.publish_changes(event_message_delete_unexisting_record)

    actual_msg = caplog.records[1].msg
    expected_msg = f'[msg-1234]: DROPPING: Fatal exception ' \
                   f'occurred when handling message: ' \
                   f'Error: {error} for changes: {changes}.'
    assert expected_msg == actual_msg


@pytest.mark.asyncio
async def test_failed_on_post_adding_bad_rrdata(
        publisher_instance, event_message,
        caplog, event_msg_data_bad_rrdata):
    """Test error is raised and the message is dropped
            when trying to add record with bad rrdata"""
    event_message.data = event_msg_data_bad_rrdata

    error = "Issue connecting to www.googleapis.com: 400, message='Bad Request'"
    changes = {'kind': 'dns#change',
               'additions':
                   [{'kind': 'dns#resourceRecordSet',
                     'name': 'service5.nurit.com.',
                     'type': 'A', 'ttl': 3600,
                     'rrdatas': ['127.10.20.899']}]
               }
    publisher_instance.http_client._request_post_mock.side_effect =\
        exceptions.GCPHTTPError(error)

    await publisher_instance.publish_changes(event_message)

    actual_msg = caplog.records[1].msg
    expected_msg = f'[msg-1234]: DROPPING: Fatal exception ' \
                   f'occurred when handling message: ' \
                   f'Error: {error} for changes: {changes}.'
    assert expected_msg == actual_msg

    msg = await publisher_instance.error_channel.get()
    assert msg == event_message


@pytest.mark.asyncio
async def test_failed_on_watch_status(
    mocker, publisher_instance, event_message,
        caplog, get_mock_coro,
        api_response_on_request_changes,
        api_response_json_on_get_json):
    """Test error is raised and messgae placed into error channel
        when timeout is reached
        on waiting for DNS changes to be done"""
    api_response_json_on_get_json['status'] = 'pending'

    publisher_instance.timeout = 2

    mock, _coroutine = get_mock_coro()
    mocker.patch('asyncio.sleep', _coroutine)

    publisher_instance.http_client._request_post_mock.return_value = \
        api_response_on_request_changes
    publisher_instance.http_client._get_json_mock.return_value =\
        api_response_json_on_get_json

    await publisher_instance.publish_changes(event_message)

    actual_msg = caplog.records[1].msg

    expected_msg = '[msg-1234]: RETRYING: ' \
                   'Exception occurred when handling message: ' \
                   'Timed out waiting for DNS changes to be done.'
    assert expected_msg == actual_msg

    # test event msg placed into error channel
    msg = await publisher_instance.error_channel.get()
    assert msg == event_message


@pytest.mark.asyncio
async def test_event_msg_placed_into_success_channel(
        publisher_instance, event_message,
        api_response_on_request_changes,
        api_response_json_on_get_json):
    """Test message placed into success channel"""
    publisher_instance.http_client._request_post_mock.return_value = \
        api_response_on_request_changes
    publisher_instance.http_client._get_json_mock.return_value = \
        api_response_json_on_get_json

    await publisher_instance.publish_changes(event_message)

    # test event msg placed into success channel
    msg = await publisher_instance.success_channel.get()
    assert msg == event_message
