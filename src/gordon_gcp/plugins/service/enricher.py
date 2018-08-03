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
"""
Client module to enrich an event message with any missing information
(such as IP addresses to a new hostname) and to generate the desired
record(s) (e.g. ``A`` or ``CNAME`` records). Once an event message is
done (either successfully enriched or met with errors along the way),
it will be placed into the appropriate channel, either the
``success_channel`` or ``error_channel`` to be further handled by the
``gordon`` core system.

.. attention::
    The enricher client is an internal module for the core gordon
    logic. No other use cases are expected.

"""

import asyncio
import logging

import zope.interface
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.clients import gdns
from gordon_gcp.clients import http
from gordon_gcp.plugins import _utils


__all__ = ('GCEEnricher', 'GCEEnricherBuilder')


class GCEEnricherBuilder:
    """Build and configure a :class:`GCEEnricher` object.

    Args:
        config (dict): Google Compute Engine API related configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        kwargs (dict): Additional keyword arguments to pass to the
            enricher.
    """
    def __init__(self, config, metrics, **kwargs):
        self.config = config
        self.metrics = metrics
        self.kwargs = kwargs
        self._validate_config()
        self.http_client = self._init_http_client()
        self.dns_client = self._init_dns_client()

    def _validate_keyfile(self):
        msg = []
        if not self.config.get('keyfile'):
            msg.append('The path to a Service Account JSON keyfile is required '
                       'to authenticate to the GCE API.')
        return msg

    def _validate_dns_zone(self):
        msg = []
        if not self.config.get('dns_zone'):
            msg.append('A dns zone is required to build correct A records.')
        if not self.config.get('dns_zone', '').endswith('.'):
            msg.append('A dns zone must be an FQDN and end with the root '
                       'zone (".").')
        return msg

    def _validate_retries(self):
        if not self.config.get('retries'):
            self.config['retries'] = 5
        return []

    def _validate_project(self):
        msg = []
        if not self.config.get('project'):
            msg.append('The GCP project that contains the Google Cloud DNS '
                       'managed zone is required to correctly delete A records '
                       'for deleted instances.')
        return msg

    def _call_validators(self):
        """Actually run all the validations.

        Returns:
            list(str): Error messages from the validators.
        """
        msg = []
        msg.extend(self._validate_keyfile())
        msg.extend(self._validate_dns_zone())
        msg.extend(self._validate_retries())
        msg.extend(self._validate_project())
        return msg

    def _validate_config(self):
        errors = []
        errors = self._call_validators()
        if errors:
            error_msgs = '\n'.join(errors)
            exp_msg = f'Invalid configuration:\n{error_msgs}'
            logging.error(error_msgs)
            raise exceptions.GCPConfigError(exp_msg)

    def _init_auth(self):
        scopes = self.config.get('scopes')
        return auth.GAuthClient(keyfile=self.config['keyfile'],
                                scopes=scopes)

    def _init_http_client(self):
        return http.AIOConnection(auth_client=self._init_auth())

    def _init_dns_client(self):
        return gdns.GDNSClient(self.config['project'], self._init_auth())

    def build_enricher(self):
        return GCEEnricher(self.config, self.metrics, self.http_client,
                           self.dns_client, **self.kwargs)


@zope.interface.implementer(interfaces.IMessageHandler)
class GCEEnricher:
    """Get needed instance information from Google Compute Engine.

    Args:
        config (dict): configuration relevant to Compute Engine.
        metrics (obj): :interface:`IMetricRelay` implementation.
        http_client (.AIOConnection): client for interacting with
            the GCE API.
    """
    phase = 'enrich'

    def __init__(self, config, metrics, http_client, dns_client, **kwargs):
        self.config = config
        self.metrics = metrics
        self._http_client = http_client
        self._dns_client = dns_client
        self._logger = logging.getLogger('')

    def _check_instance_data(self, instance_data):
        assert self._get_external_ip(instance_data)

    async def _poll_for_instance_data(self, resource_name, msg_logger):
        exception = None
        backoff = 2
        base_url = f'https://www.googleapis.com/compute/v1/{resource_name}'

        # Poll until instance data contains all necessary information.
        for attempt in range(1, self.config['retries'] + 1):
            try:
                msg_logger.debug(f'Attempt {attempt}: fetching {base_url}')
                instance_data = await self._http_client.get_json(base_url)

                self._check_instance_data(instance_data)

                return instance_data
            except exceptions.GCPHTTPError as e:
                exception = e
                break
            except (KeyError, IndexError) as e:
                exception = e
                if attempt == self.config['retries']:
                    continue
                await asyncio.sleep(backoff)
                backoff = backoff ** 2

        msg = (f'Could not get necessary information for {resource_name}: '
               f'{exception.__class__.__name__}: {exception}')
        raise exceptions.GCPGordonError(msg)

    def _create_A_rrecord(self, fqdn, external_ip, default_ttl, msg_logger):
        msg_logger.debug(f'Creating A record: {fqdn} -> {external_ip}')
        return {
            'name': fqdn,
            'rrdatas': [external_ip],
            'type': 'A',
            'ttl': default_ttl
        }

    def _get_fqdn(self, hostname):
        dns_zone = self.config['dns_zone']
        return '.'.join([hostname, dns_zone])

    def _get_external_ip(self, instance_data):
        interfaces = instance_data['networkInterfaces']
        return interfaces[0]['accessConfigs'][0]['natIP']

    def _get_internal_ip(self, instance_data):
        interfaces = instance_data['networkInterfaces']
        return interfaces[0]['networkIP']

    async def _create_rrecords(self, event_data, instance_data, msg_logger):
        # async in case anything gets added in a subclass that needs to be async
        fqdn = self._get_fqdn(instance_data['name'])
        external_ip = self._get_external_ip(instance_data)
        default_ttl = self.config['default_ttl']
        return [
            self._create_A_rrecord(
                fqdn, external_ip, default_ttl, msg_logger)
        ]

    # The types of records that this returns for deletion should match the
    # types of records that are created above in the _create_rrecords method
    async def _get_matching_records_for_deletion(self, instance_resource_url):
        # Get the fqdn of the instance
        instance_name = instance_resource_url.split('/')[-1]
        fqdn = self._get_fqdn(instance_name)

        # Get the A records with name matching the instance FQDN
        search_params = {'name': fqdn, 'type': 'A'}
        response = await self._dns_client.get_records_for_zone(
            self.config['dns_zone'], params=search_params)
        return response['rrsets']

    async def handle_message(self, event_message):
        """
        Enrich message with extra context and send it to the publisher.

        When a message is successfully processed, it is passed to the
        :obj:`self.success_channel`. However, if there is a problem
        during processing, the message is passed to the
        :obj:`self.error_channel`.

        Args:
            event_message (.GEventMessage): message requiring
                additional information.
        """
        # if the message has resource records, assume it has all info
        # it needs to be published, and therefore is already enriched
        if event_message.data['resourceRecords']:
            msg = 'Message already enriched, skipping phase.'
            event_message.append_to_history(msg, self.phase)
            return

        msg_logger = _utils.GEventMessageLogger(
            self._logger, {'msg_id': event_message.msg_id})

        if event_message.data['action'] == 'additions':
            instance_data = await self._poll_for_instance_data(
                event_message.data['resourceName'], msg_logger)
            records = await self._create_rrecords(
                event_message.data, instance_data, msg_logger)
        elif event_message.data['action'] == 'deletions':
            instance_resource_url = event_message.data['resourceName']
            records = await self._get_matching_records_for_deletion(
                instance_resource_url)

        msg_logger.debug(f'Enriched with resource record(s): {records}')
        event_message.data['resourceRecords'].extend(records)
        msg = (f"Enriched msg with {len(event_message.data['resourceRecords'])}"
               " resource record(s).")
        event_message.append_to_history(msg, self.phase)
