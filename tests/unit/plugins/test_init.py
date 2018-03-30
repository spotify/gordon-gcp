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
import json

import aiohttp
import pytest
from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub
from google.cloud.pubsub_v1.subscriber.policy import thread

from gordon_gcp import exceptions
from gordon_gcp import plugins
from gordon_gcp.clients import auth


API_BASE_URL = 'https://example.com'
API_URL = f'{API_BASE_URL}/v1/foo_endpoint'


@pytest.fixture(scope='session')
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


@pytest.fixture
def config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'project': 'test-example',
        'topic': 'a-topic',
        'subscription': 'a-subscription'
    }


@pytest.fixture
async def auth_client(mocker, monkeypatch):
    mock = mocker.Mock(auth.GAuthClient)
    mock.token = '0ldc0ffe3'
    mock._session = aiohttp.ClientSession()
    creds = mocker.Mock()
    mock.creds = creds
    monkeypatch.setattr(
        'gordon_gcp.plugins.event_consumer.auth.GAuthClient', mock)
    yield mock
    await mock._session.close()


@pytest.fixture
def subscriber_client(mocker, monkeypatch):
    mock = mocker.Mock(pubsub.SubscriberClient)
    # what is actually returned from client.create_subscription
    mock_sub = mocker.Mock(thread.Policy)
    mock.return_value.create_subscription.return_value = mock_sub
    patch = 'gordon_gcp.plugins.event_consumer.pubsub.SubscriberClient'
    monkeypatch.setattr(patch, mock)
    return mock


@pytest.fixture
def emulator(monkeypatch):
    monkeypatch.delenv('PUBSUB_EMULATOR_HOST', raising=False)


@pytest.fixture
def exp_topic(config):
    topic = config['topic'].split('/')[-1]
    return f'projects/{config["project"]}/topics/{topic}'


@pytest.fixture
def exp_sub(config):
    subscription = config['subscription'].split('/')[-1]
    return f'projects/{config["project"]}/subscriptions/{subscription}'


@pytest.mark.parametrize('local,provide_loop,topic,sub', [
    (True, True, 'a-topic',
     'projects/test-example/subscriptions/a-subscription'),
    (False, False, 'projects/test-example/topics/a-topic', 'a-subscription'),
])
def test_get_event_consumer(local, provide_loop, topic, sub, config, exp_topic,
                            auth_client, exp_sub, subscriber_client, emulator,
                            monkeypatch, event_loop):
    """Happy path to initialize a Publisher client."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    if local:
        monkeypatch.setenv('PUBSUB_EMULATOR_HOST', True)

    config['topic'], config['subscription'] = topic, sub
    kwargs = {
        'config': config,
        'success_channel': success_chnl,
        'error_channel': error_chnl,
    }
    if provide_loop:
        kwargs['loop'] = event_loop
    client = plugins.get_event_consumer(**kwargs)

    creds = None
    if not local:
        creds = auth_client.return_value.creds
    subscriber_client.assert_called_once_with(credentials=creds)
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.assert_called_once_with(exp_topic, exp_sub)

    assert client._validator
    assert client._parser
    assert client.success_channel is success_chnl
    assert client.error_channel is error_chnl

    assert client._subscriber
    assert exp_sub == client._subscription

    assert ['audit-log', 'event'] == sorted(client._message_schemas)

    if provide_loop:
        assert event_loop is client._loop
    else:
        assert event_loop is not client._loop


@pytest.mark.parametrize('config_key,exp_msg',  [
    ('keyfile', 'The path to a Service Account JSON keyfile is required '),
    ('project', 'The GCP project where Cloud Pub/Sub is located is required.'),
    ('topic', ('A topic for the client to subscribe to in Cloud Pub/Sub is '
               'required.')),
    ('subscription', ('A subscription for the client to pull messages from in '
                      'Cloud Pub/Sub is required.')),
])
def test_get_event_consumer_config_raises(config_key, exp_msg, config,
                                          auth_client, subscriber_client,
                                          caplog, emulator):
    """Raise with improper configuration."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    config.pop(config_key)

    with pytest.raises(exceptions.GCPConfigError) as e:
        client = plugins.get_event_consumer(config, success_chnl, error_chnl)
        client._subscriber.create_subscription.assert_not_called()

    e.match('Invalid configuration:\n' + exp_msg)
    assert 1 == len(caplog.records)


def test_get_event_consumer_raises_topic(config, auth_client, subscriber_client,
                                         caplog, emulator, exp_topic, exp_sub):
    """Raise when there is no topic to subscribe to."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.NotFound('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    with pytest.raises(exceptions.GCPGordonError) as e:
        plugins.get_event_consumer(config, success_chnl, error_chnl)
        sub_inst.create_subscription.assert_called_once_with(exp_topic, exp_sub)

    e.match(f'Topic "{exp_topic}" does not exist.')
    assert 3 == len(caplog.records)


def test_get_event_consumer_raises(config, auth_client, subscriber_client,
                                   caplog, emulator, exp_topic, exp_sub):
    """Raise when any other error occured with creating a subscription."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.BadRequest('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    with pytest.raises(exceptions.GCPGordonError) as e:
        plugins.get_event_consumer(config, success_chnl, error_chnl)
        sub_inst.create_subscription.assert_called_once_with(exp_topic, exp_sub)

    e.match(f'Error trying to create subscription "{exp_sub}"')
    assert 3 == len(caplog.records)


def test_get_event_consumer_sub_exists(config, auth_client, subscriber_client,
                                       emulator, exp_topic, exp_sub):
    """Do not raise if topic already exists."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.AlreadyExists('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    client = plugins.get_event_consumer(config, success_chnl, error_chnl)

    assert client._subscriber

    sub_inst.create_subscription.assert_called_once_with(exp_topic, exp_sub)
