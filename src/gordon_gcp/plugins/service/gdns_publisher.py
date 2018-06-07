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
import json
import logging
import numbers
import re

import zope.interface
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.clients import http
from gordon_gcp.plugins import _utils

__all__ = ('GDNSPublisher', 'GDNSPublisherBuilder')

BASE_CHANGES_ENDPOINT = ('https://www.googleapis.com/dns/{version}/projects/'
                         '{project}/managedZones/{managedZone}/changes')
RESOURCE_RECORDS_ENDPOINT = ('https://www.googleapis.com/dns/v1/projects/'
                             '{project}/managedZones/{managedZone}/rrsets')
# see https://cloud.google.com/dns/api/v1/changes#resource
DNS_CHANGES_DONE = 'done'


class GDNSPublisherBuilder:
    """Build and configure a :class:`GDNSPublisher` object.

    Args:
        config (dict): Google Cloud DNS API related configuration.
        success_channel (asyncio.Queue): Queue to place a successfully
            published message to be further handled by the ``gordon``
            core system.
        error_channel (asyncio.Queue): Queue to place a message met
            with errors to be further handled by the ``gordon`` core
            system.
        kwargs (dict): Additional keyword arguments to pass to the
            publisher.
    """
    def __init__(self, config, success_channel, error_channel, **kwargs):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel
        self.kwargs = kwargs
        self.validate_config_funcs = [
            self._validate_keyfile, self._validate_project,
            self._validate_dns_zone, self._validate_managed_zone,
            self._validate_publish_timeout, self._validate_default_ttl
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

    def _validate_managed_zone(self, errors):
        if not self.config.get('managed_zone'):
            msg = ('A managed zone is required to publish records to Google '
                   'Cloud DNS.')
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

    def _init_http_client(self):
        auth_client = self._init_auth_client()
        return http.AIOConnection(auth_client=auth_client)

    def build_publisher(self):
        self._validate_config()
        http_client = self._init_http_client()
        return GDNSPublisher(
            self.config, self.success_channel, self.error_channel, http_client)


@zope.interface.implementer(interfaces.IPublisherClient)
class GDNSPublisher:
    """Publish records to Google Cloud DNS.

    Args:
        config (dict): Configuration relevant to Cloud DNS.
        success_channel (asyncio.Queue): A sink for successfully
            processed :interface:`interfaces.IEventMessages`.
        error_channel (asyncio.Queue): A sink for
            :interface:`interfaces.IEventMessages` that were not
            processed due to problems.
        http_client (http.AIOConnection):
            A Google HTTP connection class.
    """
    phase = 'publish'

    def __init__(self, config, success_channel, error_channel, http_client):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel
        self.http_client = http_client
        self.publish_wait_timeout = self.config.get('publish_wait_timeout', 60)
        self.managed_zone = self.config['managed_zone']
        self.dns_zone = self.config['dns_zone']
        self.project = self.config['project']
        self.default_ttl = self.config['default_ttl']
        self.api_version = self.config.get('api_version', 'v1')
        self.base_changes_url = BASE_CHANGES_ENDPOINT.format(
            version=self.api_version, project=self.project,
            managedZone=self.managed_zone)
        self.resource_records_url = RESOURCE_RECORDS_ENDPOINT.format(
            project=self.project, managedZone=self.managed_zone)
        self._logger = logging.getLogger('')

    # TODO: This will be eventually moved to GEventMessage
    async def update_phase(self, event_msg, phase=None):
        old_phase = event_msg.phase
        event_msg.phase = phase or self.phase
        msg = f'Updated phase from "{old_phase}" to "{event_msg.phase}".'
        event_msg.append_to_history(msg, self.phase)

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

    async def _publish_changes(self, changes):
        """Publish changes to the Google DNS Changes API.

        Args:
            changes (dict): The DNS record changes to publish.

        Returns:
            change_id (str): Change ID of the Changes resource.

        Raises:
            GCPHTTPConflictError on HTTP 409.
            GCPHTTPNotFoundError on HTTP 404.
        """
        try:
            resp = await self.http_client.request(
                'post', self.base_changes_url, json=changes)
        except exceptions.GCPHTTPError as e:
            error_msg = e.args[0]
            # Extract status code from error message.
            match = re.search('\d+', error_msg)
            if match:
                status_code = int(match.group(0))
                if status_code == 409:
                    raise exceptions.GCPHTTPConflictError(e)
                elif status_code == 404:
                    raise exceptions.GCPHTTPNotFoundError(e)
            raise e

        resp_dict = json.loads(resp)

        # TODO: create another task to measure propagation time
        return resp_dict['id']

    async def _handle_additions_conflict(self, changes):
        """Compensate for an additions Changes request that returns an HTTP
           409.

            WARNING: This function fetches _all_ records in a given Managed
            Zone and may be slow if the zone contains many records.

        Args:
            changes (dict): Changes containing an addition conflict.

        Returns:
            dict containing original additions plus deletions.
        """
        resp = await self.http_client.get_all(self.resource_records_url)
        rrsets = resp['rrsets']

        record_name = changes['additions'][0]['name']
        record_type = changes['additions'][0]['type']

        for rec in rrsets:
            if rec['name'] == record_name and rec['type'] == record_type:
                changes['deletions'] = [rec]
                break

        return changes

    async def _changes_published(self, change_id):
        """Checks if a given Change resource has a completed status.

        See DNS_CHANGES_DONE.

        Args:
            change_id (str): Change ID of the Changes resource.

        Returns:
            True if the changes are done, False if anything else.
        """
        change_id_url = f'{self.base_changes_url}/{change_id}'
        timeout = datetime.timedelta(seconds=self.publish_wait_timeout)
        start = datetime.datetime.now()
        end = start + timeout

        while datetime.datetime.now() < end:
            resp = await self.http_client.get_json(change_id_url)
            if resp['status'] == DNS_CHANGES_DONE:
                return True
            await asyncio.sleep(1)

        return False

    async def _dispatch_changes(self, resource_record, action, logger):
        """Publish changes for one record, making sure they are completed.

        Args:
            resource_record (dict): A resource record to change.
            action: The action to take (e.g. 'additions').
            logger: An object to log with.

        Raises:
            GCPPublishRecordTimeoutError if publishing of records exceeds wait
                timeout.
        """
        changes_to_publish = self._format_resource_record_changes(
            action, resource_record)
        try:
            change_id = await self._publish_changes(changes_to_publish)
        except exceptions.GCPHTTPConflictError as e:
            msg = ('Conflict found when publishing records. Handling and '
                   'retrying.')
            logger.info(msg)
            changes_to_publish = await self._handle_additions_conflict(
                changes_to_publish)
            change_id = await self._publish_changes(changes_to_publish)

        change_complete = await self._changes_published(change_id)
        if not change_complete:
            msg = ('Timed out while waiting for DNS changes to transition '
                   'to \'done\' status.')
            logger.error(msg)
            raise exceptions.GCPPublishRecordTimeoutError(msg)
        logger.info('Records successfully published.')

    async def publish_changes(self, event_msg):
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

        for resource_record in event_msg.data['resourceRecords']:
            record_name = resource_record['name']
            if not record_name.endswith('.' + self.dns_zone):
                msg = f'Error when asserting zone for record: {record_name}.'
                msg_logger.error(msg)
                raise exceptions.InvalidDNSZoneInMessageError(msg)

            await self._dispatch_changes(
                resource_record, event_msg.data['action'], msg_logger)
