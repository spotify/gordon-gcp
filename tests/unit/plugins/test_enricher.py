# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 Spotify AB
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

import pytest  # NOQA
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins import enricher


@pytest.fixture
def instance_data(audit_log_data):
    return {
        'name': audit_log_data['resourceName'].split('/')[-1],
        'networkInterfaces': [{
            'accessConfigs': [{
                'natIP': '127.99.199.27'
            }],
        }]
    }


@pytest.fixture
def config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'scopes': [],
        'dns_zone': 'example.com.',
        'default_ttl': 300,
        'retries': 5
    }


def test_implements_interface(config):
    """GCEEnricher implements IEnricherClient"""
    success, error = asyncio.Queue(), asyncio.Queue()
    client = enricher.GCEEnricher(config, None, success, error)

    assert interfaces.IEnricherClient.providedBy(client)
    assert interfaces.IEnricherClient.implementedBy(enricher.GCEEnricher)
    assert config is client.config
    assert success is client.success_channel
    assert error is client.error_channel
    assert 'enrich' == client.phase


@pytest.fixture
def mock_http_client(mocker, get_mock_coro):
    get_json_mock, get_json_coro = get_mock_coro()
    http_client = mocker.Mock()
    mocker.patch.object(http_client, 'get_json', get_json_coro)
    mocker.patch.object(http_client, '_get_json_mock', get_json_mock)
    return http_client


@pytest.fixture
def mock_async_sleep(mocker, get_mock_coro):
    sleep_mock, sleep_coro = get_mock_coro()
    mocker.patch('gordon_gcp.plugins.enricher.asyncio.sleep', sleep_coro)
    return sleep_mock


@pytest.mark.parametrize('sleep_calls,logs_logged', [
    (0, 3),
    (2, 5)])
@pytest.mark.asyncio
async def test_process_event_msg(config, gevent_msg, channel_pair,
                                 mock_async_sleep, mock_http_client, caplog,
                                 instance_data, sleep_calls, logs_logged):
    """Successfully enrich event message and put on the success channel."""
    success_chnl, error_chnl = channel_pair
    instance_mocked_data = []
    for i in range(sleep_calls):
        instance_mocked_data.append({})
    instance_mocked_data.append(instance_data)
    mock_http_client._get_json_mock.side_effect = instance_mocked_data

    gce_enricher = enricher.GCEEnricher(config, mock_http_client, success_chnl,
                                        error_chnl)

    await gce_enricher.process(gevent_msg)

    ret_event_message = await success_chnl.get()

    assert 'publish' == ret_event_message.phase
    expected_history_msg = 'Enriched msg with 1 resource record(s).'
    assert expected_history_msg == ret_event_message.history_log[0]['message']
    assert 'enrich' == ret_event_message.history_log[0]['plugin']
    assert 1 == len(ret_event_message.data['resourceRecords'])
    expected_rrecords = [{
        'name': '.'.join([
            gevent_msg.data['resourceName'].split('/')[-1], config['dns_zone']
        ]),
        'type': 'A',
        'rrdatas': [
            instance_data['networkInterfaces'][0]['accessConfigs'][0][
                'natIP']
        ],
        'ttl': config['default_ttl']
    }]
    assert expected_rrecords == ret_event_message.data['resourceRecords']
    assert sleep_calls == mock_async_sleep.call_count
    assert logs_logged == len(caplog.records)


@pytest.mark.parametrize('response,sleep_calls,logs_logged,err_msg', [
    (exceptions.GCPHTTPError('404 error'), 0, 2, 'GCPHTTPError: 404 error'),
    ([{}] * 5, 4, 6, 'KeyError: \'networkInterfaces\'')])
@pytest.mark.asyncio
async def test_process_event_msg_failures(config, gevent_msg, channel_pair,
                                          mock_async_sleep, mock_http_client,
                                          caplog, response, sleep_calls,
                                          logs_logged, err_msg):
    """Handle and log error while enriching event message."""
    success_chnl, error_chnl = channel_pair
    mock_http_client._get_json_mock.side_effect = response
    gce_enricher = enricher.GCEEnricher(config, mock_http_client, success_chnl,
                                        error_chnl)
    gevent_msg.phase = 'enrich'

    await gce_enricher.process(gevent_msg)

    ret_event_message = await error_chnl.get()

    assert success_chnl.empty()
    assert 'enrich' == ret_event_message.phase
    assert gevent_msg == ret_event_message
    assert sleep_calls == mock_async_sleep.call_count
    assert logs_logged == len(caplog.records)
    assert 1 == len(ret_event_message.history_log)
    assert 'enrich' == ret_event_message.history_log[0]['plugin']
    expected_history_msg = ('Encountered error while enriching message, '
                            'sending message to error channel: Could not get '
                            'necessary information for '
                            'projects/123456789101/zones/us-central1-c/'
                            f'instances/an-instance-name-b34c: {err_msg}.')
    assert expected_history_msg == ret_event_message.history_log[0]['message']
