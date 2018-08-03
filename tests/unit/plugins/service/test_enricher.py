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
def instance_data(creation_audit_log_data):
    return {
        'name': creation_audit_log_data['resourceName'].split('/')[-1],
        'networkInterfaces': [{
            'accessConfigs': [{
                'natIP': '127.99.199.27'
            }],
        }]
    }


@pytest.fixture
def records_data(config, deletions_gevent_msg):
    fqdn = ('.'.join([deletions_gevent_msg.data['resourceName']
                     .split('/')[-1], config['dns_zone']]))
    return [
        {
            'kind': 'dns#resourceRecordSet',
            'name': fqdn,
            'type': 'A',
            'ttl': 300,
            'rrdatas': ['127.0.0.1']
        }
    ]


@pytest.fixture
def get_json_response_data(records_data):
    return {
        'kind': 'dns#resourceRecordSetsListResponse',
        'rrsets': records_data
    }


@pytest.fixture
def config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'scopes': [],
        'dns_zone': 'example.com.',
        'project': 'gcp-proj-dns',
        'default_ttl': 300,
        'retries': 5
    }


@pytest.fixture
def mock_http_client(mocker, create_mock_coro):
    get_json_mock, get_json_coro = create_mock_coro()
    http_client = mocker.Mock()
    mocker.patch.object(http_client, 'get_json', get_json_coro)
    mocker.patch.object(http_client, '_get_json_mock', get_json_mock)
    return http_client


@pytest.fixture
def mock_dns_client(mocker, create_mock_coro, records_data):
    mock, coro = create_mock_coro()
    mock.return_value = {'rrsets': records_data}
    client = mocker.Mock()
    mocker.patch.object(client, 'get_records_for_zone', coro)
    mocker.patch.object(client, '_get_records_for_zone_mock', mock)
    return client


@pytest.fixture
def mock_async_sleep(mocker, create_mock_coro):
    sleep_mock, sleep_coro = create_mock_coro()
    mocker.patch(
        'gordon_gcp.plugins.service.enricher.asyncio.sleep', sleep_coro)
    return sleep_mock


@pytest.fixture
def gce_enricher(config, metrics, mock_http_client, mock_dns_client):
    return enricher.GCEEnricher(
        config, metrics, mock_http_client, mock_dns_client)


def test_implements_interface(config, gce_enricher):
    """GCEEnricher implements IMessageHandler"""
    assert interfaces.IMessageHandler.providedBy(gce_enricher)
    assert interfaces.IMessageHandler.implementedBy(enricher.GCEEnricher)
    assert config is gce_enricher.config
    assert 'enrich' == gce_enricher.phase


@pytest.mark.asyncio
async def test_handle_message_doesnt_need_enriching(
        mocker, caplog, additions_gevent_msg, gce_enricher):
    additions_gevent_msg.data['resourceRecords'] = [mocker.Mock()]
    expected_msg = 'Message already enriched, skipping phase.'
    await gce_enricher.handle_message(additions_gevent_msg)
    assert expected_msg == additions_gevent_msg.history_log[0]['message']
    assert 0 == len(caplog.records)


@pytest.mark.parametrize('sleep_calls,logs_logged', [
    (0, 3),
    (2, 5)])
@pytest.mark.asyncio
async def test_handle_message_event_msg_additions(
        mocker, config, additions_gevent_msg, mock_async_sleep, gce_enricher,
        caplog, instance_data, sleep_calls, logs_logged):
    """Successfully enrich event message with records to add."""
    instance_mocked_data = []
    for i in range(sleep_calls):
        instance_mocked_data.append({})
    instance_mocked_data.append(instance_data)

    gce_enricher._http_client._get_json_mock.side_effect = instance_mocked_data

    await gce_enricher.handle_message(additions_gevent_msg)

    expected_history_msg = 'Enriched msg with 1 resource record(s).'
    assert (expected_history_msg ==
            additions_gevent_msg.history_log[0]['message'])
    assert 'enrich' == additions_gevent_msg.history_log[0]['plugin']
    assert 1 == len(additions_gevent_msg.data['resourceRecords'])
    expected_rrecords = [{
        'name': '.'.join([
            additions_gevent_msg.data['resourceName'].split('/')[-1],
            config['dns_zone']
        ]),
        'type': 'A',
        'rrdatas': [
            instance_data['networkInterfaces'][0]['accessConfigs'][0][
                'natIP']
        ],
        'ttl': config['default_ttl']
    }]
    assert expected_rrecords == additions_gevent_msg.data['resourceRecords']
    assert sleep_calls == mock_async_sleep.call_count
    assert logs_logged == len(caplog.records)


@pytest.mark.parametrize('response,sleep_calls,logs_logged,err_msg', [
    (exceptions.GCPHTTPResponseError('404 error', 404), 0, 1,
        'GCPHTTPResponseError: 404 error'),
    ([{}] * 5, 4, 5, 'KeyError: \'networkInterfaces\'')])
@pytest.mark.asyncio
async def test_handle_message_event_msg_additions_failures(
        mocker, additions_gevent_msg, mock_async_sleep, gce_enricher, caplog,
        response, sleep_calls, logs_logged, err_msg):
    """Raise error while enriching event message with additions."""
    gce_enricher._http_client._get_json_mock.side_effect = response
    additions_gevent_msg.phase = 'enrich'

    with pytest.raises(exceptions.GCPGordonError) as e:
        await gce_enricher.handle_message(additions_gevent_msg)

    assert sleep_calls == mock_async_sleep.call_count
    assert logs_logged == len(caplog.records)
    expected_msg = ('Could not get necessary information for '
                    'projects/123456789101/zones/us-central1-c/'
                    f'instances/an-instance-name-b34c: {err_msg}')
    assert e.match(expected_msg)


@pytest.mark.asyncio
async def test_handle_message_event_msg_deletions(
        mocker, config, deletions_gevent_msg, gce_enricher, caplog,
        records_data, get_json_response_data):
    """Successfully enrich event message with records to delete."""
    gce_enricher._http_client._get_json_mock.return_value = (
        get_json_response_data)

    await gce_enricher.handle_message(deletions_gevent_msg)

    expected_history_msg = 'Enriched msg with 1 resource record(s).'
    expected_rrecords = records_data
    assert (expected_history_msg ==
            deletions_gevent_msg.history_log[0]['message'])
    assert 'enrich' == deletions_gevent_msg.history_log[0]['plugin']
    assert expected_rrecords == deletions_gevent_msg.data['resourceRecords']
