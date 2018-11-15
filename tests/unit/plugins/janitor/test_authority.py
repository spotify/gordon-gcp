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
import copy
import logging
from unittest import mock

import pytest

from gordon_gcp import exceptions
from gordon_gcp.clients import gce
from gordon_gcp.clients import gcrm
from gordon_gcp.plugins.janitor import authority


def echoing_helper_coro(data):
    async def _coro():
        return data
    return _coro()


@pytest.fixture
def fake_authority(mocker, monkeypatch, authority_config, auth_client, metrics):
    monkeypatch.setattr(
        'gordon_gcp.plugins.janitor.authority.auth.GAuthClient',
        mocker.Mock(return_value=auth_client))
    rrset_channel = asyncio.Queue()
    fake_authority = authority.GCEAuthority(
        authority_config, metrics, None, rrset_channel)
    fake_authority.session = auth_client._session
    return fake_authority


@pytest.fixture
def get_mock_client(mocker, get_gce_client):
    def _create_fake(klass, *args, **kwargs):
        client = get_gce_client(klass, *args, **kwargs)
        fake_client = mocker.Mock(client)
        return fake_client
    return _create_fake


@pytest.mark.asyncio
async def test_builder_creates_proper_authority(
        mocker, authority_config, metrics):
    gce_client = mocker.MagicMock(name='gce_client')
    crm_client = mocker.MagicMock(name='crm_client')
    auth_client = mocker.MagicMock(name='auth_client')
    authority_mod_patch = 'gordon_gcp.clients.'
    mocker.patch(authority_mod_patch + 'gcrm.GCRMClient', crm_client)
    mocker.patch(authority_mod_patch + 'gce.GCEClient', gce_client)
    mocker.patch('gordon_gcp.clients.auth.GAuthClient', auth_client)

    rrset_channel = asyncio.Queue()
    kwargs = {'other': 'kwargs'}
    builder = authority.GCEAuthorityBuilder(
        authority_config, metrics, rrset_channel, **kwargs)
    try:
        gce_authority = builder.build_authority()
    finally:
        builder.session.close()

    auth_client_calls = [
        mocker.call(
            authority_config['keyfile'],
            scopes=authority_config['scopes'],
            session=builder.session),
    ] * 2

    auth_client.assert_has_calls(auth_client_calls, any_order=True)
    expected_metadata_blacklist = [
        dict([pair]) for pair in authority_config['metadata_blacklist']
    ]
    gce_client.assert_called_once_with(
        auth_client(),
        builder.session,
        blacklisted_tags=authority_config['tag_blacklist'],
        blacklisted_metadata=expected_metadata_blacklist)

    assert crm_client() is gce_authority.crm_client
    assert gce_client() is gce_authority.gce_client
    assert rrset_channel is gce_authority.rrset_channel
    assert authority_config is gce_authority.config


@pytest.mark.asyncio
async def test_run_publishes_msg_to_channel(mocker, authority_config,
                                            get_gce_client, create_mock_coro,
                                            instance_data, metrics):
    instances = []
    for i in range(1, 4):
        inst = copy.deepcopy(instance_data)
        inst['name'] = f'host-{i}'
        inst['networkInterfaces'][0]['accessConfigs'][0]['natIp'] = f'1.1.1.{i}'
        instances.append(inst)

    active_projects_mock, active_projects_coro = create_mock_coro()
    active_projects_mock.return_value = [{'projectId': 1}, {'projectId': 2}]
    crm_client = get_gce_client(gcrm.GCRMClient)
    crm_client.list_all_active_projects = active_projects_coro

    list_instances_mock, list_instances_coro = create_mock_coro()
    list_instances_mock.return_value = instances
    gce_client = get_gce_client(gce.GCEClient)
    gce_client.list_instances = list_instances_coro

    rrset_channel = asyncio.Queue()
    gce_authority = authority.GCEAuthority(
        authority_config, metrics, crm_client, gce_client, rrset_channel)

    await gce_authority.run()

    _expected_rrsets = []
    for instance in instances:
        ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        _expected_rrsets.append({
            'name': f"{instance['name']}.{authority_config['dns_zone']}",
            'type': 'A',
            'rrdatas': [ip]})
    expected_rrsets = _expected_rrsets * 2
    expected_msg = {
        'zone': authority_config['dns_zone'],
        'rrsets': expected_rrsets
    }

    # one message includes data for all projects
    assert expected_msg == (await rrset_channel.get())
    # run also calls self.cleanup at the end
    assert (await rrset_channel.get()) is None

    context = {'plugin': 'gceauthority'}
    gce_authority.metrics._timer_mock.assert_called_once_with(
        'plugin-runtime', context=context)

    gce_authority.metrics.timer_stub.start_mock.assert_called_once_with()
    gce_authority.metrics.timer_stub.stop_mock.assert_called_once_with()
    context = {
        'plugin': 'gceauthority',
    }
    gce_authority.metrics._set_mock.assert_has_calls([mock.call(
        'projects', 2, context=context)])

    context = {
        'plugin': 'gceauthority',
        'zone': authority_config['dns_zone'],
    }
    gce_authority.metrics._set_mock.assert_has_calls([mock.call(
        'rrsets', len(expected_rrsets), context=context)])


@pytest.mark.asyncio
async def test_run_continues_when_404_error_raised(
        mocker, caplog, authority_config, get_gce_client, create_mock_coro,
        instance_data, metrics):
    caplog.set_level(logging.WARN)

    instances = [instance_data]
    active_projects_mock, active_projects_coro = create_mock_coro()
    active_projects_mock.return_value = [{'projectId': 1}, {'projectId': 2}]
    crm_client = get_gce_client(gcrm.GCRMClient)
    crm_client.list_all_active_projects = active_projects_coro

    list_instances_mock, list_instances_coro = create_mock_coro()
    list_instances_mock.side_effect = [
        exceptions.GCPHTTPResponseError('Error!', 404), instances]
    gce_client = get_gce_client(gce.GCEClient)
    gce_client.list_instances = list_instances_coro

    mock_rrset_channel = mocker.Mock()
    rrset_channel_put_mock, rrset_channel_put_coro = create_mock_coro()
    mock_rrset_channel.put = rrset_channel_put_coro
    gce_authority = authority.GCEAuthority(
        authority_config, metrics, crm_client, gce_client, mock_rrset_channel)

    await gce_authority.run()

    # warning log project was skipped
    assert 1 == len(caplog.records)
    assert 2 == rrset_channel_put_mock.call_count


@pytest.mark.parametrize('exception_args,exception', [
    (('Server is too busy to respond', 502), exceptions.GCPHTTPResponseError),
    (('An unhandled error!',), Exception)
])
@pytest.mark.asyncio
async def test_run_fails_when_unexpected_error_raised(
        mocker, authority_config, get_gce_client, create_mock_coro,
        instance_data, exception_args, exception, metrics):
    instances = [instance_data]
    active_projects_mock, active_projects_coro = create_mock_coro()
    active_projects_mock.return_value = [{'projectId': 1}, {'projectId': 2}]
    crm_client = get_gce_client(gcrm.GCRMClient)
    crm_client.list_all_active_projects = active_projects_coro

    list_instances_mock, list_instances_coro = create_mock_coro()
    list_instances_mock.side_effect = [
        exception(*exception_args), instances]
    gce_client = get_gce_client(gce.GCEClient)
    gce_client.list_instances = list_instances_coro

    mock_rrset_channel = mocker.Mock()
    rrset_channel_put_mock, rrset_channel_put_coro = create_mock_coro()
    mock_rrset_channel.put = rrset_channel_put_coro
    gce_authority = authority.GCEAuthority(
        authority_config, metrics, crm_client, gce_client, mock_rrset_channel)

    with pytest.raises(exception):
        await gce_authority.run()

    rrset_channel_put_mock.assert_not_called()


def test_create_msgs_bad_json(caplog, fake_authority):
    """Authority ignores incomplete instance data."""
    caplog.set_level(logging.WARN)
    partial_instance = {'name': 'incomplete-instance-1'}
    results = fake_authority._create_msgs([partial_instance])
    assert [] == results
    assert 1 == len(caplog.records)


@pytest.mark.parametrize('whitelist,expected', [
    (['whitelisted_1', 'whitelisted_2'], ['whitelisted_1', 'whitelisted_2']),
    (None, ["project_1", "project_2"])
])
@pytest.mark.asyncio
async def test_get_projects_with_whitelist(
        mocker, whitelist, expected, authority_config,
        get_gce_client, create_mock_coro, metrics):
    active_projects_mock, active_projects_coro = create_mock_coro()
    active_projects_mock.return_value = [{'projectId': "project_1"},
                                         {'projectId': "project_2"}]
    crm_client = get_gce_client(gcrm.GCRMClient)
    crm_client.list_all_active_projects = active_projects_coro

    gce_client = get_gce_client(gce.GCEClient)

    mock_rrset_channel = mocker.Mock()

    authority_config['project_whitelist'] = whitelist

    gce_authority = authority.GCEAuthority(
        authority_config, metrics, crm_client, gce_client, mock_rrset_channel)

    actual = await gce_authority._get_projects()

    assert actual == expected
