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

from gordon_gcp.plugins.service import enricher
from gordon_gcp.plugins.service import event_consumer
from gordon_gcp.plugins.service import gdns_publisher
# Mainly for easier documentation reading
from gordon_gcp.plugins.service.enricher import *  # noqa: F401,F403
from gordon_gcp.plugins.service.event_consumer import *  # noqa: F401,F403
from gordon_gcp.plugins.service.gdns_publisher import *  # noqa: F401,F403


__all__ = (
    enricher.__all__ +  # noqa: F405
    event_consumer.__all__ +  # noqa: F405
    gdns_publisher.__all__ +  # noqa: F405
    ('get_event_consumer', 'get_enricher', 'get_gdns_publisher')
)


def get_event_consumer(config, success_channel, error_channel, metrics,
                       **kwargs):
    """Get a GPSEventConsumer client.

    A factory function that validates configuration, creates schema
    validator and parser clients, creates an auth and a pubsub client,
    and returns an event consumer (:interface:`gordon.interfaces.
    IRunnable` and :interface:`gordon.interfaces.IMessageHandler`)
    provider.

    Args:
        config (dict): Google Cloud Pub/Sub-related configuration.
        success_channel (asyncio.Queue): Queue to place a successfully
            consumed message to be further handled by the ``gordon``
            core system.
        error_channel (asyncio.Queue): Queue to place a message met
            with errors to be further handled by the ``gordon`` core
            system.
        metrics (obj): :interface:`IMetricRelay` implementation.
        kwargs (dict): Additional keyword arguments to pass to the
            event consumer.
    Returns:
        A :class:`GPSEventConsumer` instance.
    """
    builder = event_consumer.GPSEventConsumerBuilder(
        config, success_channel, error_channel, metrics, **kwargs)
    return builder.build_event_consumer()


def get_enricher(config, metrics, **kwargs):
    """Get a GCEEnricher client.

    A factory function that validates configuration and returns an
    enricher client (:interface:`gordon.interfaces.IMessageHandler`)
    provider.

    Args:
        config (dict): Google Compute Engine API related configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        kwargs (dict): Additional keyword arguments to pass to the
            enricher.
    Returns:
        A :class:`GCEEnricher` instance.
    """
    builder = enricher.GCEEnricherBuilder(
        config, metrics, **kwargs)
    return builder.build_enricher()


def get_gdns_publisher(config, metrics, **kwargs):
    """Get a GDNSPublisher client.

    A factory function that validates configuration and returns a
    publisher client (:interface:`gordon.interfaces.IMessageHandler`)
    provider.

    Args:
        config (dict): Google Cloud DNS API related configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        kwargs (dict): Additional keyword arguments to pass to the
            publisher.
    Returns:
        A :class:`GDNSPublisher` instance.
    """
    builder = gdns_publisher.GDNSPublisherBuilder(
        config, metrics, **kwargs)
    return builder.build_publisher()
