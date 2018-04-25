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
import aiohttp
import pytest  # NOQA
from google.cloud import pubsub_v1
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins import publisher
from gordon_gcp.clients import auth, http
from gordon_gcp.plugins import event_consumer
import os

os.environ['PYTHONASYNCIODEBUG'] = '1'


@pytest.fixture
def config():
    return {
        'timeout': 90,
        'valid_zones': ['nurit-com'],
        'project': 'pr-tower-hackweek'
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
def event_message(mocker, event_msg_data, pubsub_message):
    event_msg = mocker.MagicMock(event_consumer.GEventMessage)
    event_msg.msg_id = pubsub_message.message_id
    event_msg.data = event_msg_data
    event_message.phase = ''
    return event_msg


@pytest.mark.parametrize('provide_session', [True, False])
def test_implements_interface(provide_session, mocker, config):
    """GDNSPublisher implements IPublisherClient"""

    session = None
    if provide_session:
        session = aiohttp.ClientSession()

    auth_client = mocker.Mock(auth.GAuthClient)
    auth_client._session = aiohttp.ClientSession()
    creds = mocker.Mock()
    auth_client.creds = creds
    client = http.AIOConnection(auth_client=auth_client, session=session)

    success, error = asyncio.Queue(), asyncio.Queue()
    client = publisher.GDNSPublisher(config, success, error, client)

    assert interfaces.IPublisherClient.providedBy(client)
    assert interfaces.IPublisherClient.implementedBy(publisher.GDNSPublisher)
    assert config is client.config
    assert success is client.success_channel
    assert error is client.error_channel
    assert 'publish' == client.phase


def test_find_zone_failed(
        publisher_instance, event_message,
        event_msg_data_with_invalid_zone):
    event_message.data = event_msg_data_with_invalid_zone

    record = event_message.data['resourceRecords'][0]

    with pytest.raises(exceptions.GCPGordonError) as e:
        publisher_instance.publish_changes(event_message)

    expected_msg = f'Error trying to find zone ' \
                   f'in valid_zone for record: {record}'

    assert e.match(expected_msg)


def test_invalid_action(
        publisher_instance, event_message,
        event_msg_data_with_invalid_action):
    event_message.data = event_msg_data_with_invalid_action

    action = event_message.data['action']

    with pytest.raises(exceptions.GCPGordonError) as e:
        publisher_instance.publish_changes(event_message)

    expected_msg = f'Error trying to format changes, ' \
                   f'got an invalid action: {action}'

    assert e.match(expected_msg)


@pytest.mark.asyncio
async def test_publish_changes_failed_on_post_adding_existing_record(
        event_message, mock_http_client, config, caplog):
    expected_changes = {'kind': 'dns#change',
                        'additions': [{'kind': 'dns#resourceRecordSet',
                                       'name': 'service3.nurit.com.',
                                       'type': 'A', 'ttl': 3600,
                                       'rrdatas': ['127.10.20.2']}]}

    expected_error = "Issue connecting to www.googleapis.com: 409, message='Conflict'"

    mock_http_client._request_post_mock.side_effect = exceptions.GCPHTTPError(expected_error)

    success, error = asyncio.Queue(), asyncio.Queue()
    pb = publisher.GDNSPublisher(config, success, error, mock_http_client)

    await pb.publish_changes(event_message)
    failed_msg = caplog.records[1].msg
    print(failed_msg)

    # expected_msg = "[msg-1234]: Encountered a retryable error: " \
    #                "Issue connecting to www.googleapis.com: 409, message='Conflict'"
    # assert expected_msg == failed_msg


@pytest.mark.asyncio
async def test_publish_changes_failed_on_watch_status():
    pass


@pytest.mark.asyncio
async def test_event_msg_placed_into_error_channel():
    pass


@pytest.mark.asyncio
async def test_event_msg_placed_into_success_channel():
    pass

