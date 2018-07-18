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

import zope.interface
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.clients import http
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
            self.config, self.metrics, http_client, **self.kwargs)


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
        http_client (http.AIOConnection):
            A Google HTTP connection class.
    """
    phase = 'publish'
    BASE_CHANGES_ENDPOINT = ('https://www.googleapis.com/dns/{version}'
                             '/projects/{project}/managedZones/{managedZone}'
                             '/changes')
    RESOURCE_RECORDS_ENDPOINT = ('https://www.googleapis.com/dns/v1/projects/'
                                 '{project}/managedZones/{managedZone}/rrsets')
    # see https://cloud.google.com/dns/api/v1/changes#resource
    DNS_CHANGES_DONE = 'done'

    def __init__(self, config, metrics, http_client, **kwargs):
        self.config = config
        self.http_client = http_client
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

    async def _publish_changes(self, changes, base_changes_url):
        """Publish changes to the Google DNS Changes API.

        Args:
            changes (dict): The DNS record changes to publish.
            base_changes_url (str): The URL to use to publish changes.

        Returns:
            change_id (str): Change ID of the Changes resource.
        """
        resp = await self.http_client.request(
            'post', base_changes_url, json=changes)
        resp_dict = json.loads(resp)

        # TODO: create another task to measure propagation time
        return resp_dict['id']

    async def _handle_additions_conflict(self, changes, resource_records_url):
        """Compensate for an additions Changes request that returns an HTTP
           409.

            WARNING: This function fetches _all_ records in a given Managed
            Zone and may be slow if the zone contains many records.

        Args:
            changes (dict): Changes containing an addition conflict.
            resource_records_url (str): The URL to use to get the zone's
                resource records.

        Returns:
            dict containing original additions plus deletions.
        """
        record_name = changes['additions'][0]['name']
        record_type = changes['additions'][0]['type']
        search_params = {
            'name': record_name,
            'type': record_type
        }
        response = await self.http_client.get_json(
            resource_records_url, params=search_params)
        if response['rrsets']:
            changes['deletions'] = response['rrsets']
        return changes

    async def _changes_published(self, change_id, base_changes_url):
        """Checks if a given Change resource has a completed status.

        See DNS_CHANGES_DONE.

        Args:
            change_id (str): Change ID of the Changes resource.
            base_changes_url (str): The URL prefix to use to check on changes.

        Returns:
            True if the changes are done, False if anything else.
        """
        change_id_url = f'{base_changes_url}/{change_id}'
        timeout = datetime.timedelta(seconds=self.publish_wait_timeout)
        start = datetime.datetime.now()
        end = start + timeout

        while datetime.datetime.now() < end:
            resp = await self.http_client.get_json(change_id_url)
            if resp['status'] == GDNSPublisher.DNS_CHANGES_DONE:
                return True
            await asyncio.sleep(1)

        return False

    async def _dispatch_changes(self, resource_record, managed_zone, action,
                                logger):
        """Publish changes for one record, making sure they are completed.

        Args:
            resource_record (dict): A resource record to change.
            managed_zone (str): The managed zone name in Google DNS.
            action (str): The action to take (e.g. 'additions').
            logger (object): An object to log with.

        Raises:
            GCPPublishRecordTimeoutError if publishing of records exceeds wait
                timeout.
        """
        base_changes_url = GDNSPublisher.BASE_CHANGES_ENDPOINT.format(
            version=self.api_version, project=self.project,
            managedZone=managed_zone)
        resource_records_url = GDNSPublisher.RESOURCE_RECORDS_ENDPOINT.format(
            project=self.project, managedZone=managed_zone)

        changes_to_publish = self._format_resource_record_changes(
            action, resource_record)
        try:
            change_id = await self._publish_changes(
                changes_to_publish, base_changes_url)

        except exceptions.GCPHTTPResponseError as e:
            if e.status != 409:
                raise e

            msg = ('Conflict found when publishing records. Handling and '
                   'retrying.')
            logger.info(msg)
            changes_to_publish = await self._handle_additions_conflict(
                changes_to_publish, resource_records_url)
            change_id = await self._publish_changes(changes_to_publish,
                                                    base_changes_url)

        change_complete = await self._changes_published(change_id,
                                                        base_changes_url)
        if not change_complete:
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
                    resource_record, self.config['managed_zone'],
                    event_msg.data['action'], msg_logger)
        else:
            msg = ('No records published or deleted as no resource records were'
                   ' present.')
            msg_logger.info(msg)
            event_msg.append_to_history(msg, self.phase)
