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

from gordon_gcp.plugins import enricher
from gordon_gcp.plugins import event_consumer
# Mainly for easier documentation reading
from gordon_gcp.plugins.enricher import *  # noqa: F403
from gordon_gcp.plugins.event_consumer import *  # noqa: F403
from gordon_gcp.plugins.publisher import *  # noqa: F403

__all__ = (
    enricher.__all__ +  # noqa: F405
    event_consumer.__all__ +  # noqa: F405
    publisher.__all__ +  # noqa: F405
    ('get_event_consumer', 'get_enricher')
)


def get_event_consumer(config, success_channel, error_channel, **kwargs):
    """Get a GPSEventConsumer client.

    A factory function that validates configuration, creates schema
    validator and parser clients, creates an auth and a pubsub client,
    and returns an event consumer (:interface:`gordon.interfaces.
    IEventConsumerClient`) provider.

    Args:
        config (dict): Google Cloud Pub/Sub-related configuration.
        success_channel (asyncio.Queue): queue to place a successfully
            consumed message to be further handled by the ``gordon``
            core system.
        error_channel (asyncio.Queue): queue to place a message met
            with errors to be further handled by the ``gordon`` core
            system.
        kwargs (dict): Additional keyword arguments to pass to the
            event consumer.
    Returns:
        A :class:`GPSEventConsumer` instance.
    """
    builder = event_consumer.GPSEventConsumerBuilder(
        config, success_channel, error_channel, **kwargs)
    return builder.build_event_consumer()


def get_enricher(config, success_channel, error_channel, **kwargs):
    """Get a GCEEnricher client.

    A factory function that validates configuration and returns an
    enricher client (:interface:`gordon.interfaces.IEnricherClient`)
    provider.

    Args:
        config (dict): Google Compute Engine API related configuration.
        success_channel (asyncio.Queue): queue to place a successfully
            enriched message to be further handled by the ``gordon``
            core system.
        error_channel (asyncio.Queue): queue to place a message met
            with errors to be further handled by the ``gordon`` core
            system.
        kwargs (dict): Additional keyword arguments to pass to the
            enricher.
    Returns:
        A :class:`GCEEnricher` instance.
    """
    builder = enricher.GCEEnricherBuilder(
        config, success_channel, error_channel, **kwargs)
    return builder.build_enricher()
