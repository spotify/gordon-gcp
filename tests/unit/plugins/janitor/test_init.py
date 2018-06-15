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

import pytest
from google.api_core import exceptions as google_exceptions

from gordon_gcp import exceptions
from gordon_gcp.plugins import janitor


@pytest.mark.parametrize('local,timeout,exp_timeout,topic', [
    (True, None, 60, 'a-topic'),
    (False, 30, 30, 'projects/test-example/topics/a-topic'),
])
def test_get_gpubsub_publisher(local, timeout, exp_timeout, topic, config,
                               auth_client, emulator, mock_pubsub_client,
                               monkeypatch):
    """Happy path to initialize a GPubsubPublisher client."""
    changes_chnl = asyncio.Queue()

    if local:
        monkeypatch.setenv('PUBSUB_EMULATOR_HOST', True)

    if timeout:
        config['cleanup_timeout'] = timeout

    config['topic'] = topic
    client = janitor.get_gpubsub_publisher(config, changes_chnl)

    topic = topic.split('/')[-1]
    exp_topic = f'projects/{config["project"]}/topics/{topic}'
    assert exp_timeout == client.cleanup_timeout
    assert client.publisher is not None
    assert not client._messages

    client.publisher.create_topic.assert_called_once_with(exp_topic)


@pytest.mark.parametrize('config_key,exp_msg', [
    ('keyfile', 'The path to a Service Account JSON keyfile is required '),
    ('project', 'The GCP project where Cloud Pub/Sub is located is required.'),
    ('topic', ('A topic for the client to publish to in Cloud Pub/Sub is '
               'required.')),
])
def test_get_gpubsub_publisher_config_raises(
        config_key, exp_msg, config, auth_client, mock_pubsub_client,
        caplog, emulator):
    """Raise with improper configuration."""
    changes_chnl = asyncio.Queue()
    config.pop(config_key)

    with pytest.raises(exceptions.GCPConfigError) as e:
        client = janitor.get_gpubsub_publisher(config, changes_chnl)
        client.publisher.create_topic.assert_not_called()

    e.match(exp_msg)
    assert 1 == len(caplog.records)


def test_get_gpubsub_publisher_raises(
        config, auth_client, mock_pubsub_client, caplog, emulator):
    """Raise when there's an issue creating a Google Pub/Sub topic."""
    changes_chnl = asyncio.Queue()
    mock_pubsub_client.return_value.create_topic.side_effect = [
        Exception('fooo')
    ]

    with pytest.raises(exceptions.GCPGordonJanitorError) as e:
        client = janitor.get_gpubsub_publisher(config, changes_chnl)

        client.publisher.create_topic.assert_called_once_with(client.topic)
        e.match(f'Error trying to create topic "{client.topic}"')

    assert 1 == len(caplog.records)


def test_get_gpubsub_publisher_topic_exists(
        config, auth_client, mock_pubsub_client, emulator):
    """Do not raise if topic already exists."""
    changes_chnl = asyncio.Queue()
    exp = google_exceptions.AlreadyExists('foo')
    mock_pubsub_client.return_value.create_topic.side_effect = [exp]

    short_topic = config['topic']
    client = janitor.get_gpubsub_publisher(config, changes_chnl)

    exp_topic = f'projects/{config["project"]}/topics/{short_topic}'
    assert 60 == client.cleanup_timeout
    assert client.publisher is not None
    assert not client._messages
    assert exp_topic == client.topic

    client.publisher.create_topic.assert_called_once_with(exp_topic)


@pytest.mark.parametrize('timeout,exp_timeout', [
    (None, 60),
    (30, 30),
])
def test_get_reconciler(timeout, exp_timeout, config, auth_client, monkeypatch):
    """Happy path to initialize a Reconciler client."""
    rrset_chnl = asyncio.Queue()
    changes_chnl = asyncio.Queue()

    if timeout:
        config['cleanup_timeout'] = timeout

    client = janitor.get_reconciler(config, rrset_chnl, changes_chnl)

    assert exp_timeout == client.cleanup_timeout
    assert client.dns_client is not None
    assert rrset_chnl == client.rrset_channel
    assert changes_chnl == client.changes_channel


@pytest.mark.parametrize('key,error_msg', [
    ('keyfile', 'The path to a Service Account JSON keyfile is required '),
    ('project', 'The GCP project where Cloud DNS is located is required.')
])
def test_get_reconciler_config_raises(key, error_msg, config, auth_client,
                                      caplog):
    """Raise with improper configuration."""
    rrset_chnl = asyncio.Queue()
    changes_chnl = asyncio.Queue()
    config.pop(key)

    with pytest.raises(exceptions.GCPConfigError) as e:
        janitor.get_reconciler(config, rrset_chnl, changes_chnl)

    e.match(error_msg)
    assert 1 == len(caplog.records)


@pytest.mark.asyncio
async def test_get_authority(authority_config, auth_client):
    """Test authority client initialization happy path."""
    rrset_channel = asyncio.Queue()

    client = janitor.get_authority(authority_config, rrset_channel)
    assert rrset_channel == client.rrset_channel
    assert client.crm_client is not None
    assert client.gce_client is not None

    # cleaning up; only need to close one client's session since they
    # are the same object
    await client.crm_client._session.close()


@pytest.mark.parametrize('config_key,error_msg', [
    ('keyfile', 'The path to a Service Account JSON keyfile is required '),
    ('dns_zone', 'The absolute DNS zone, i.e. "example.com.", is required ')])
def test_get_authority_config_raises(caplog, config_key, error_msg,
                                     authority_config, auth_client):
    """Raise with bad configuration."""
    rrset_channel = asyncio.Queue()
    del authority_config[config_key]
    with pytest.raises(exceptions.GCPConfigError) as e:
        janitor.get_authority(authority_config, rrset_channel)

    e.match(error_msg)
    assert 1 == len(caplog.records)
