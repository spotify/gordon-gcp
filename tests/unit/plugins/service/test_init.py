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


from gordon_gcp import exceptions
from gordon_gcp.plugins import service


#####
# service.get_event_consumer tests
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
    mock_sub = mocker.Mock()
    mock.return_value.create_subscription.return_value = mock_sub
    patch = 'gordon_gcp.plugins.service.event_consumer.pubsub.SubscriberClient'
    monkeypatch.setattr(patch, mock)
    return mock


@pytest.fixture
def exp_topic(consumer_config):
    topic = consumer_config['topic'].split('/')[-1]
    return f'projects/{consumer_config["project"]}/topics/{topic}'


@pytest.fixture
def exp_sub(consumer_config):
    subscription = consumer_config['subscription'].split('/')[-1]
    return f'projects/{consumer_config["project"]}/subscriptions/{subscription}'


@pytest.mark.parametrize('local,provide_loop,topic,sub', [
    (True, True, 'a-topic',
     'projects/test-example/subscriptions/a-subscription'),
    (False, False, 'projects/test-example/topics/a-topic', 'a-subscription'),
])
def test_get_event_consumer(local, provide_loop, topic, sub, consumer_config,
                            exp_topic, auth_client, exp_sub, subscriber_client,
                            emulator, monkeypatch, event_loop, metrics):
    """Happy path to initialize an Event Consumer client."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    if local:
        monkeypatch.setenv('PUBSUB_EMULATOR_HOST', True)

    consumer_config['topic'], consumer_config['subscription'] = topic, sub
    kwargs = {
        'config': consumer_config,
        'success_channel': success_chnl,
        'error_channel': error_chnl,
        'metrics': metrics
    }
    if provide_loop:
        kwargs['loop'] = event_loop
    client = service.get_event_consumer(**kwargs)

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
                                          caplog, emulator, metrics):
    """Raise with improper configuration."""
    consumer_config.pop(config_key)
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()
    with pytest.raises(exceptions.GCPConfigError) as e:
        service.get_event_consumer(
            consumer_config, success_chnl, error_chnl, metrics)
    subscriber_client.create_subscription.assert_not_called()

    e.match('Invalid configuration:\n' + exp_msg)
    assert 1 == len(caplog.records)


def test_get_event_consumer_raises_topic(consumer_config, auth_client,
                                         subscriber_client, caplog, emulator,
                                         exp_topic, exp_sub, metrics):
    """Raise when there is no topic to subscribe to."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.NotFound('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    with pytest.raises(exceptions.GCPGordonError) as e:
        service.get_event_consumer(
            consumer_config, success_chnl, error_chnl, metrics)
    sub_inst.create_subscription.assert_called_once_with(exp_sub, exp_topic)

    e.match(f'Topic "{exp_topic}" does not exist.')
    assert 3 == len(caplog.records)


def test_get_event_consumer_raises(consumer_config, auth_client,
                                   subscriber_client, caplog, emulator,
                                   exp_topic, exp_sub, metrics):
    """Raise when any other error occured with creating a subscription."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.BadRequest('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    with pytest.raises(exceptions.GCPGordonError) as e:
        service.get_event_consumer(
            consumer_config, success_chnl, error_chnl, metrics)
    sub_inst.create_subscription.assert_called_once_with(exp_sub, exp_topic)

    e.match(f'Error trying to create subscription "{exp_sub}"')
    assert 3 == len(caplog.records)


def test_get_event_consumer_sub_exists(consumer_config, auth_client,
                                       subscriber_client, emulator, exp_topic,
                                       exp_sub, metrics):
    """Do not raise if topic already exists."""
    success_chnl, error_chnl = asyncio.Queue(), asyncio.Queue()

    exp = google_exceptions.AlreadyExists('foo')
    sub_inst = subscriber_client.return_value
    sub_inst.create_subscription.side_effect = [exp]

    client = service.get_event_consumer(consumer_config, success_chnl,
                                        error_chnl, metrics)

    assert client._subscriber

    sub_inst.create_subscription.assert_called_once_with(exp_sub, exp_topic)


#####
# service.get_enricher tests
#####
@pytest.fixture
def enricher_config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'dns_zone': 'example.com.',
        'managed_zone': 'example-com',
        'project': 'gcp-proj-dns',
    }


@pytest.mark.parametrize('conf_retries,retries', [
    (None, 5),
    (10, 10)])
def test_get_enricher(mocker, enricher_config, auth_client, conf_retries,
                      retries, metrics):
    """Happy path to initialize an Enricher client."""
    mocker.patch('gordon_gcp.plugins.service.enricher.http.AIOConnection')
    enricher_config['retries'] = conf_retries

    client = service.get_enricher(enricher_config, metrics)

    assert isinstance(client, service.enricher.GCEEnricher)
    assert client.config
    assert retries == client.config['retries']
    assert client._http_client


@pytest.mark.parametrize('config_key,exc_msg', [
    ('keyfile', 'The path to a Service Account JSON keyfile is required to '
                'authenticate to the GCE API.'),
    ('dns_zone', 'A dns zone is required to build correct A records.'),
    ('managed_zone', 'The name of the Google Cloud DNS managed zone is '
                     'required to correctly delete A records for deleted '
                     'instances'),
    ('project', 'The GCP project that contains the Google Cloud DNS managed '
                'zone is required to correctly delete A records for deleted '
                'instances.')])
def test_get_enricher_missing_config_raises(mocker, caplog, enricher_config,
                                            auth_client, config_key, exc_msg,
                                            metrics):
    """Raise when configuration key is missing."""
    mocker.patch('gordon_gcp.plugins.service.enricher.http.AIOConnection')
    enricher_config.pop(config_key)

    with pytest.raises(exceptions.GCPConfigError) as e:
        service.get_enricher(enricher_config, metrics)

    e.match('Invalid configuration:\n' + exc_msg)
    assert 1 == len(caplog.records)


def test_get_enricher_config_bad_dns_zone(mocker, caplog, enricher_config,
                                          auth_client, metrics):
    """Raise when 'dns_zone' config value doesn't end with root zone."""
    mocker.patch('gordon_gcp.plugins.service.enricher.http.AIOConnection')
    enricher_config['dns_zone'] = 'example.com'

    with pytest.raises(exceptions.GCPConfigError) as e:
        service.get_enricher(enricher_config, metrics)

    exc_msg = 'A dns zone must be an FQDN and end with the root zone \("."\).'
    e.match('Invalid configuration:\n' + exc_msg)
    assert 1 == len(caplog.records)


#####
# service.get_gdns_publisher tests
#####
@pytest.fixture
def publisher_config(fake_keyfile):
    return {
        'keyfile': fake_keyfile,
        'dns_zone': 'example.com.',
        'project': 'test-example',
        'managed_zone': 'my-zone',
        'default_ttl': 300,
    }


@pytest.mark.parametrize('conf_key,conf_value,expected', (
    # publish_wait_timeout explicitly set
    ('publish_wait_timeout', 30, 30),
    # publish_wait_timeout not set
    ('publish_wait_timeout', False, 60),
    # api_version explicitly set
    ('api_version', 'v1beta', 'v1beta'),
    # # version not set
    ('api_version', False, 'v1'),
))
def test_get_gdns_publisher(conf_key, conf_value, expected, mocker,
                            publisher_config, auth_client, metrics):
    """Happy path to initialize a GDNSPublisher client."""
    patch = 'gordon_gcp.plugins.service.gdns_publisher.http.AIOConnection'
    mocker.patch(patch)

    if conf_value:
        publisher_config[conf_key] = conf_value

    client = service.get_gdns_publisher(publisher_config, metrics)

    assert isinstance(client, service.gdns_publisher.GDNSPublisher)
    assert publisher_config == client.config
    assert expected == getattr(client, conf_key)


@pytest.mark.parametrize('conf_keys,exp_msg_snip', (
    (('keyfile',), ('The path to a Service Account JSON keyfile is required '
                    'to authenticate for Google Cloud DNS.')),
    (('project',), 'The GCP project where Cloud DNS is located is required.'),
    (('dns_zone',), 'A dns zone is required to build correct A records.'),
    (('managed_zone',), ('A managed zone is required to publish records to '
                         'Google Cloud DNS.')),
    (('default_ttl',), ('A default TTL in seconds must be set for publishing '
                        'records to Google Cloud DNS.')),
    (('keyfile', 'project'), ('The path to a Service Account JSON keyfile is '
                              'required to authenticate for Google Cloud DNS.\n'
                              'The GCP project where Cloud DNS is located is '
                              'required.\n'))
))
def test_get_gdns_publisher_raises(conf_keys, exp_msg_snip,
                                   publisher_config, mocker, auth_client,
                                   caplog, metrics):
    """Raise when required config key(s) missing."""
    patch = 'gordon_gcp.plugins.service.gdns_publisher.http.AIOConnection'
    mocker.patch(patch)

    for conf_key in conf_keys:
        publisher_config.pop(conf_key)

    with pytest.raises(exceptions.GCPConfigError) as e:
        service.get_gdns_publisher(publisher_config, metrics)

    e.match('Invalid configuration:\n' + exp_msg_snip)
    assert len(conf_keys) == len(caplog.records)


@pytest.mark.parametrize('conf_key,bad_value,exp_msg_snip', (
    ('dns_zone', 'example.com', ('A dns zone must be an FQDN and end with the '
                                 'root zone \("."\).')),
    ('publish_wait_timeout', 'foo', ('"publish_wait_timeout" must be a '
                                     'positive number.')),
    ('publish_wait_timeout', -1, ('"publish_wait_timeout" must be a positive '
                                  'number.')),
    ('default_ttl', 'foo', '"default_ttl" must be an integer.'),
    ('default_ttl', -1, '"default_ttl" must be greater than 4'),
))
def test_get_gdns_publisher_bad_config(conf_key, bad_value, exp_msg_snip,
                                       publisher_config, mocker, auth_client,
                                       caplog, metrics):
    """Raise when config values are malformed."""
    patch = 'gordon_gcp.plugins.service.gdns_publisher.http.AIOConnection'
    mocker.patch(patch)

    publisher_config[conf_key] = bad_value

    with pytest.raises(exceptions.GCPConfigError) as e:
        service.get_gdns_publisher(publisher_config, metrics)

    e.match('Invalid configuration:\n' + exp_msg_snip)
    assert 1 == len(caplog.records)
