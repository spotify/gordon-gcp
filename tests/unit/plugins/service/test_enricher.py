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

import pytest  # NOQA
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins.service import enricher


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


def test_implements_interface(mocker, config):
    """GCEEnricher implements IEnricherClient"""
    success, error = mocker.Mock(), mocker.Mock()
    client = enricher.GCEEnricher(config, None, success, error)

    assert interfaces.IEnricherClient.providedBy(client)
    assert interfaces.IEnricherClient.implementedBy(enricher.GCEEnricher)
    assert config is client.config
    assert 'enrich' == client.phase


@pytest.fixture
def mock_http_client(mocker, create_mock_coro):
    get_json_mock, get_json_coro = create_mock_coro()
    http_client = mocker.Mock()
    mocker.patch.object(http_client, 'get_json', get_json_coro)
    mocker.patch.object(http_client, '_get_json_mock', get_json_mock)
    return http_client


@pytest.fixture
def mock_async_sleep(mocker, create_mock_coro):
    sleep_mock, sleep_coro = create_mock_coro()
    mocker.patch(
        'gordon_gcp.plugins.service.enricher.asyncio.sleep', sleep_coro)
    return sleep_mock


@pytest.mark.asyncio
async def test_process_doesnt_need_processing(mocker, caplog, config,
                                              gevent_msg):
    gce_enricher = enricher.GCEEnricher(config, mocker.Mock(), None, None)
    gevent_msg.data['resourceRecords'] = [mocker.Mock()]
    expected_msg = 'Message already enriched, skipping phase.'
    await gce_enricher.process(gevent_msg)
    assert expected_msg == gevent_msg.history_log[0]['message']
    assert 0 == len(caplog.records)


@pytest.mark.parametrize('sleep_calls,logs_logged', [
    (0, 3),
    (2, 5)])
@pytest.mark.asyncio
async def test_process_event_msg(mocker, config, gevent_msg, mock_async_sleep,
                                 mock_http_client, caplog, instance_data,
                                 sleep_calls, logs_logged):
    """Successfully enrich event message."""
    instance_mocked_data = []
    for i in range(sleep_calls):
        instance_mocked_data.append({})
    instance_mocked_data.append(instance_data)
    mock_http_client._get_json_mock.side_effect = instance_mocked_data
    mock_channel = mocker.Mock()

    gce_enricher = enricher.GCEEnricher(config, mock_http_client,
                                        mock_channel, mock_channel)

    await gce_enricher.process(gevent_msg)

    expected_history_msg = 'Enriched msg with 1 resource record(s).'
    assert expected_history_msg == gevent_msg.history_log[0]['message']
    assert 'enrich' == gevent_msg.history_log[0]['plugin']
    assert 1 == len(gevent_msg.data['resourceRecords'])
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
    assert expected_rrecords == gevent_msg.data['resourceRecords']
    assert sleep_calls == mock_async_sleep.call_count
    assert logs_logged == len(caplog.records)


@pytest.mark.parametrize('response,sleep_calls,logs_logged,err_msg', [
    (exceptions.GCPHTTPError('404 error'), 0, 1, 'GCPHTTPError: 404 error'),
    ([{}] * 5, 4, 5, 'KeyError: \'networkInterfaces\'')])
@pytest.mark.asyncio
async def test_process_event_msg_failures(mocker, config, gevent_msg,
                                          mock_async_sleep, mock_http_client,
                                          caplog, response, sleep_calls,
                                          logs_logged, err_msg):
    """Raise error while enriching event message."""
    mock_http_client._get_json_mock.side_effect = response
    mock_channel = mocker.Mock()
    gce_enricher = enricher.GCEEnricher(config, mock_http_client, mock_channel,
                                        mock_channel)
    gevent_msg.phase = 'enrich'

    with pytest.raises(exceptions.GCPGordonError) as e:
        await gce_enricher.process(gevent_msg)

    assert 'enrich' == gevent_msg.phase
    assert sleep_calls == mock_async_sleep.call_count
    assert logs_logged == len(caplog.records)
    expected_msg = ('Could not get necessary information for '
                    'projects/123456789101/zones/us-central1-c/'
                    f'instances/an-instance-name-b34c: {err_msg}')
    assert e.match(expected_msg)
