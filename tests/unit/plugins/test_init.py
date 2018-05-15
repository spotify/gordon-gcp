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

import pytest
from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub
from google.cloud.pubsub_v1.subscriber.policy import thread

from gordon_gcp import exceptions
from gordon_gcp import plugins


#####
# plugins.get_event_consumer tests
#####
@pytest.fixture
def consumer_config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'project': 'test-example',
        'topic': 'a-topic',
        'subscription': 'a-subscription'
    }


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
def exp_topic(consumer_config):
    topic = consumer_config['topic'].split('/')[-1]
    return f'projects/{consumer_config["project"]}/topics/{topic}'


@pytest.fixture
def exp_sub(consumer_config):
    subscription = consumer_config['subscription'].split('/')[-1]
    return f'projects/{consumer_config["project"]}/subscriptions/{subscription}'


# For some reason, the event loop for this test leaks over to
# tests.unit.plugins.test_event_consumer:test_gpsthread_add_task so
# there's a warning of not awaiting the coroutine (which is expected).
# Threads + asyncio + testing is hard.
@pytest.mark.filterwarnings('ignore:coroutine')
@pytest.mark.parametrize('local,provide_loop,topic,sub', [
    (True, True, 'a-topic',
     'projects/test-example/subscriptions/a-subscription'),
    (False, False, 'projects/test-example/topics/a-topic', 'a-subscription'),
])
def test_get_event_consumer(local, provide_loop, topic, sub, consumer_config,
                            exp_topic, auth_client, exp_sub, subscriber_client,
                            emulator, monkeypatch, event_loop):
    """Happy path to initialize a Publisher client."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    if local:
        monkeypatch.setenv('PUBSUB_EMULATOR_HOST', True)

    consumer_config['topic'], consumer_config['subscription'] = topic, sub
    kwargs = {
        'config': consumer_config,
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
    sub_inst.create_subscription.assert_called_once_with(exp_sub, exp_topic)

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
def test_get_event_consumer_config_raises(config_key, exp_msg, consumer_config,
                                          auth_client, subscriber_client,
                                          caplog, emulator):
    """Raise with improper configuration."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    consumer_config.pop(config_key)

    with pytest.raises(exceptions.GCPConfigError) as e:
        client = plugins.get_event_consumer(consumer_config, success_chnl,
                                            error_chnl)
        client._subscriber.create_subscription.assert_not_called()

    e.match('Invalid configuration:\n' + exp_msg)
    assert 1 == len(caplog.records)


def test_get_event_consumer_raises_topic(consumer_config, auth_client,
                                         subscriber_client, caplog, emulator,
                                         exp_topic, exp_sub):
    """Raise when there is no topic to subscribe to."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.NotFound('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    with pytest.raises(exceptions.GCPGordonError) as e:
        plugins.get_event_consumer(consumer_config, success_chnl, error_chnl)
        sub_inst.create_subscription.assert_called_once_with(exp_sub, exp_topic)

    e.match(f'Topic "{exp_topic}" does not exist.')
    assert 3 == len(caplog.records)


def test_get_event_consumer_raises(consumer_config, auth_client,
                                   subscriber_client, caplog, emulator,
                                   exp_topic, exp_sub):
    """Raise when any other error occured with creating a subscription."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.BadRequest('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    with pytest.raises(exceptions.GCPGordonError) as e:
        plugins.get_event_consumer(consumer_config, success_chnl, error_chnl)
        sub_inst.create_subscription.assert_called_once_with(exp_sub, exp_topic)

    e.match(f'Error trying to create subscription "{exp_sub}"')
    assert 3 == len(caplog.records)


def test_get_event_consumer_sub_exists(consumer_config, auth_client,
                                       subscriber_client, emulator, exp_topic,
                                       exp_sub):
    """Do not raise if topic already exists."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.AlreadyExists('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    client = plugins.get_event_consumer(consumer_config, success_chnl,
                                        error_chnl)

    assert client._subscriber

    sub_inst.create_subscription.assert_called_once_with(exp_sub, exp_topic)


#####
# plugins.get_enricher tests
#####
@pytest.fixture
def enricher_config(fake_keyfile):
    return {'keyfile': fake_keyfile, 'dns_zone': 'example.com.'}


@pytest.mark.parametrize('conf_retries,retries', [
    (None, 5),
    (10, 10)])
def test_get_enricher(mocker, enricher_config, auth_client, conf_retries,
                      retries):
    """Happy path to initialize an Enricher client."""
    mocker.patch('gordon_gcp.plugins.enricher.http.AIOConnection')
    enricher_config['retries'] = conf_retries

    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    client = plugins.get_enricher(enricher_config, success_chnl, error_chnl)

    assert isinstance(client, plugins.enricher.GCEEnricher)
    assert client.config
    assert retries == client.config['retries']
    assert client.success_channel
    assert client.error_channel
    assert client._http_client


@pytest.mark.parametrize('config_key,exc_msg', [
    ('keyfile', 'The path to a Service Account JSON keyfile is required to '
                'authenticate to the GCE API.'),
    ('dns_zone', 'A dns zone is required to build correct A records.')])
def test_get_enricher_missing_config_raises(mocker, caplog, enricher_config,
                                            auth_client, config_key, exc_msg):
    """Raise when configuration key is missing."""
    mocker.patch('gordon_gcp.plugins.enricher.http.AIOConnection')
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()
    enricher_config.pop(config_key)

    with pytest.raises(exceptions.GCPConfigError) as e:
        plugins.get_enricher(enricher_config, success_chnl, error_chnl)

    e.match('Invalid configuration:\n' + exc_msg)
    assert 1 == len(caplog.records)


def test_get_enricher_config_bad_dns_zone(mocker, caplog, enricher_config,
                                          auth_client):
    """Raise when 'dns_zone' config value doesn't end with root zone."""
    mocker.patch('gordon_gcp.plugins.enricher.http.AIOConnection')
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()
    enricher_config['dns_zone'] = 'example.com'

    with pytest.raises(exceptions.GCPConfigError) as e:
        plugins.get_enricher(enricher_config, success_chnl, error_chnl)

    exc_msg = 'A dns zone must be an FQDN and end with the root zone \("."\).'
    e.match('Invalid configuration:\n' + exc_msg)
    assert 1 == len(caplog.records)
