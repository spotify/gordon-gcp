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

"""
Client module to publish DNS records from an event message. Once an event
message is done (either successfully published, or met with errors along the
way), it will placed into the appropriate channel, either the
`success_channel`` or ``error_channel`` to be further handled by the ``gordon``
core system.

.. attention::
    The publisher client is an internal module for the core gordon logic. No
    other use cases are expected.
"""

import asyncio
import datetime
import logging
import numbers

import zope.interface
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.clients import gdns
from gordon_gcp.plugins import _utils


__all__ = ('GDNSPublisher', 'GDNSPublisherBuilder')


class GDNSPublisherBuilder:
    """Build and configure a :class:`GDNSPublisher` object.

    Args:
        config (dict): Google Cloud DNS API related configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        kwargs (dict): Additional keyword arguments to pass to the
            publisher.
    """
    def __init__(self, config, metrics, **kwargs):
        self.config = config
        self.metrics = metrics
        self.kwargs = kwargs
        self.validate_config_funcs = [
            self._validate_keyfile,
            self._validate_project,
            self._validate_dns_zone,
            self._validate_publish_timeout,
            self._validate_default_ttl
        ]

    def _validate_keyfile(self, errors):
        # TODO (lynn): keyfile won't be required once we support other
        #              auth methods
        if not self.config.get('keyfile'):
            msg = ('The path to a Service Account JSON keyfile is required to '
                   'authenticate for Google Cloud DNS.')
            errors.append(msg)

    def _validate_project(self, errors):
        if not self.config.get('project'):
            msg = 'The GCP project where Cloud DNS is located is required.'
            errors.append(msg)

    def _validate_dns_zone(self, errors):
        if not self.config.get('dns_zone'):
            msg = 'A dns zone is required to build correct A records.'
            errors.append(msg)
        elif not self.config.get('dns_zone', '').endswith('.'):
            msg = ('A dns zone must be an FQDN and end with the root zone '
                   '(".").')
            errors.append(msg)

    def _validate_publish_timeout(self, errors):
        if 'publish_wait_timeout' in self.config:
            timeout = self.config.get('publish_wait_timeout')
            if not isinstance(timeout, numbers.Number) or timeout < 0:
                msg = '"publish_wait_timeout" must be a positive number."'
                errors.append(msg)

    def _validate_default_ttl(self, errors):
        if not self.config.get('default_ttl'):
            msg = ('A default TTL in seconds must be set for publishing '
                   'records to Google Cloud DNS.')
            errors.append(msg)
        else:
            try:
                ttl = int(self.config.get('default_ttl'))
            except (ValueError, TypeError):
                msg = '"default_ttl" must be an integer.'
                errors.append(msg)
            else:
                # NOTE: GDNS API accepts 0-4, but defaults to a minimum of 5.
                if ttl < 5:
                    msg = '"default_ttl" must be greater than 4.'
                    errors.append(msg)

    def _validate_config(self):
        errors = []

        for validate_func in self.validate_config_funcs:
            validate_func(errors)

        if errors:
            exc_msg = 'Invalid configuration:\n'
            for error in errors:
                logging.error(error)
                exc_msg += error + '\n'
            raise exceptions.GCPConfigError(exc_msg)

    def _init_auth_client(self):
        scopes = self.config.get('scopes')
        return auth.GAuthClient(
            keyfile=self.config['keyfile'], scopes=scopes)

    def _init_dns_client(self):
        auth_client = self._init_auth_client()
        return gdns.GDNSClient(
            self.config['project'], auth_client,
            default_zone_prefix=self.config.get('default_zone_prefix', ''))

    def build_publisher(self):
        self._validate_config()
        dns_client = self._init_dns_client()
        return GDNSPublisher(
            self.config, self.metrics, dns_client, **self.kwargs)


@zope.interface.implementer(interfaces.IMessageHandler)
class GDNSPublisher:
    """Publish records to Google Cloud DNS.

    Args:
        config (dict): Configuration relevant to Cloud DNS.
        success_channel (asyncio.Queue): A sink for successfully
            processed :interface:`interfaces.IEventMessages`.
        error_channel (asyncio.Queue): A sink for
            :interface:`interfaces.IEventMessages` that were not
            processed due to problems.
        dns_client (gdns.GDNSClient):
            A Google DNS HTTP connection class.
    """
    phase = 'publish'

    def __init__(self, config, metrics, dns_client, **kwargs):
        self.config = config
        self.dns_client = dns_client
        self.metrics = metrics
        self.publish_wait_timeout = self.config.get('publish_wait_timeout', 60)
        self.project = self.config['project']
        self.default_ttl = self.config['default_ttl']
        self.api_version = self.config.get('api_version', 'v1')
        self._logger = logging.getLogger('')

    def _format_resource_record_changes(self, action, resource_record):
        """Return dict containing the changes to be made.

        Args:
            action (str): The action of the changes to publish;
                can be one of ['additions', 'deletions'].
            resource_record (dict): Dict containing record data.

        Returns:
            The changes (dict) to post to the Google API.
        """
        resource_record['kind'] = 'dns#resourceRecordSet'
        resource_record['ttl'] = resource_record.get('ttl', self.default_ttl)
        return {
            'kind': 'dns#change',
            action: [resource_record]
        }

    async def _get_rrsets_by_name_and_type(self, zone, rrset):
        """Get rrsets from GDNS matching a given rrsets's name and type.

        Args:
            zone (str): The DNS zone name.
            rrset (dict): The rrset from which to get the name and type.

        Returns:
            (list[dict]): The matching rrsets.
        """
        record_name = rrset['name']
        record_type = rrset['type']
        search_params = {
            'name': record_name,
            'type': record_type
        }
        return await self.dns_client.get_records_for_zone(
            zone, params=search_params)

    async def _is_change_done(self, zone, change_id):
        """Checks if a Change resource has completed (within the timeout).

        Args:
            zone (str): The DNS zone name.
            change_id (str): Change ID of the Changes resource.
        Returns:
            True if the changes are done, False if anything else.
        """
        timeout = datetime.timedelta(seconds=self.publish_wait_timeout)
        start = datetime.datetime.now()
        end = start + timeout

        while datetime.datetime.now() < end:
            if await self.dns_client.is_change_done(zone, change_id):
                return True
            await asyncio.sleep(1)

        return False

    async def _dispatch_changes(self, resource_record, zone, action,
                                logger):
        """Publish changes for one record, making sure they are completed.

        Args:
            resource_record (dict): A resource record to change.
            zone (str): The DNS zone name.
            action (str): The action to take (e.g. 'additions').
            logger (object): An object to log with.

        Raises:
            GCPPublishRecordTimeoutError if publishing of records exceeds wait
                timeout.
        """
        changes = self._format_resource_record_changes(
            action, resource_record)
        try:
            change_id = await self.dns_client.publish_changes(zone, changes)
        except exceptions.GCPHTTPResponseError as e:
            if e.status != 409:
                raise e

            msg = ('Conflict found when publishing records. Handling and '
                   'retrying.')
            logger.info(msg)

            # get the records GDNS has for this name and type
            deletions = await self._get_rrsets_by_name_and_type(
                zone, changes['additions'][0])
            changes['deletions'] = deletions
            change_id = await self.dns_client.publish_changes(zone, changes)

        if not await self._is_change_done(zone, change_id):
            msg = ('Timed out while waiting for DNS changes to transition '
                   'to \'done\' status.')
            logger.error(msg)
            raise exceptions.GCPPublishRecordTimeoutError(msg)
        logger.info('Records successfully published.')

    async def handle_message(self, event_msg):
        """Publish changes extracted from the event message.

        Args:
            event_msg (event_consumer.GEventMessage):
                Contains the changes to publish.

        Raises:
            InvalidDNSZoneInMessageError if the DNS zone of a resource record
                does not match our configured zone.
        """
        msg_logger = _utils.GEventMessageLogger(
            self._logger, {'msg_id': event_msg.msg_id})
        msg_logger.info('Publisher received new message.')

        if event_msg.data['resourceRecords']:
            for resource_record in event_msg.data['resourceRecords']:
                record_name = resource_record['name']
                if not record_name.endswith('.' + self.config['dns_zone']):
                    msg = ('Error when asserting zone for record: '
                           f'{record_name}.')
                    msg_logger.error(msg)
                    raise exceptions.InvalidDNSZoneInMessageError(msg)

                await self._dispatch_changes(
                    resource_record, self.config['dns_zone'],
                    event_msg.data['action'], msg_logger)
        else:
            msg = ('No records published or deleted as no resource records were'
                   ' present.')
            msg_logger.info(msg)
            event_msg.append_to_history(msg, self.phase)
