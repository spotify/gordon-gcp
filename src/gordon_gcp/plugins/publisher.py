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
Client module to publish DNS records from an event message. Once an
event message is done (either successfully published, or met with
errors along the way), it will placed into the appropriate channel,
either the ``success_channel`` or ``error_channel`` to be further
handled by the ``gordon`` core system.

.. attention::
    The publisher client is an internal module for the core gordon
    logic. No other use cases are expected.
"""
import asyncio
import datetime
import json
import logging
import re

import zope.interface
from gordon import interfaces

from gordon_gcp import exceptions
from gordon_gcp.plugins import _utils

__all__ = ('GDNSPublisher',)

HOST = 'https://www.googleapis.com'
BASE_CHANGES_ENDPOINT = \
    '{host}/dns/{version}/projects/{project}/managedZones/{managedZone}/changes'
RESOURCE_RECORDS_ENDPOINT = \
    '{host}/dns/v1/projects/{project}/managedZones/{managedZone}/rrsets'


@zope.interface.implementer(interfaces.IPublisherClient)
class GDNSPublisher:
    """Publish records to Google Cloud DNS.

    Args:
        config (dict): configuration relevant to Cloud DNS.
        success_channel (asyncio.Queue): a sink for successfully
            processed :interface:`interfaces.IEventMessages`.
        error_channel (asyncio.Queue): a sink for
            :interface:`interfaces.IEventMessages` that were not
            processed due to problems.
        http_client (http.AIOConnection):
            A google http connection class
    """
    phase = 'publish'

    def __init__(self, config, success_channel, error_channel, http_client):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel
        self.http_client = http_client
        self.timeout = config['timeout']
        self.managed_zone = config['managed_zone']
        self.dns_zone = config['dns_zone']
        self.project = config['project']
        self.default_ttl = self.config.get('default_ttl', 300)
        _api_version = config.get('version', 'v1')
        self.base_url = BASE_CHANGES_ENDPOINT.format(
            host=HOST, version=_api_version, project=self.project,
            managedZone=self.managed_zone)
        self._logger = logging.getLogger('')

    # TODO: This will be eventually moved to GEventMessage
    async def update_phase(self, event_msg, phase=None):
        old_phase = event_msg.phase
        event_msg.phase = phase or self.phase
        msg = f'Updated phase from "{old_phase}" to "{event_msg.phase}".'
        event_msg.append_to_history(msg, self.phase)

    def _format_changes(self, action, resource_record):
        """Return dict containing the changes to be made.

        Args:
            action (str): the action of the changes to publish,
                can be one of ['additions', 'deletions'].
            resource_record (dict): dict contains record data.

        Returns:
            The changes (dict) to post to google API.
        """
        resource_record['kind'] = 'dns#resourceRecordSet'
        resource_record['ttl'] = resource_record.get('ttl', self.default_ttl)

        return {
            'kind': 'dns#change',
            action: [resource_record]
        }

    async def _watch_status(self, changes_id):
        """Check google API if the changes are done.

        Args:
            changes_id (int): check the status for the changes_id.

        Returns:
            True if the changes are done.

        Raises:
            Raise error if timeout passed and
                we stiLl didn't get status "Done" from the API.
        """
        url = f'{self.base_url}/{changes_id}'

        timeout = datetime.timedelta(seconds=self.timeout)
        start = datetime.datetime.now()
        end = start + timeout

        while datetime.datetime.now() < end:
            resp = await self.http_client.get_json(url)

            if resp['status'] == 'done':
                return True

            await asyncio.sleep(1)

        msg = 'Timed out waiting for DNS changes to be done'
        raise exceptions.GCPRetryMessageError(msg)

    async def _publish_changes(self, changes, rec=False):
        """Post changes to the google API and watch request status.

        Args:
            changes (dict): the changes to make.
            rec (bool): flag to call _handle_update only once.

        Returns:
            Boolean if the changes are done.
        """
        try:
            resp = await self.http_client.request('post', self.base_url,
                                                  json=changes)
        except Exception as e:
            error_msg = e.args[0]
            # extract status code from error message
            match = re.search('\d+', error_msg)
            if match:
                status_code = int(match.group(0))

                if status_code == 409 and not rec:
                    await self._handle_update(changes)
                    return
                elif 400 <= status_code < 500:
                    msg = f'Error: {e} for changes: {changes}'
                    raise exceptions.GCPDropMessageError(msg)
            raise e

        resp_dict = json.loads(resp)

        # TODO: create another task to measure propagation time
        return await self._watch_status(resp_dict['id'])

    async def _handle_update(self, changes):
        """Handle update changes to record.

        Get the existing record from the API,
        append deletion action with the record to the changes,
        then publish changes again.

        Args:
            changes (dict): add the deletions action of
                the existing record to the changes.
        """
        url = RESOURCE_RECORDS_ENDPOINT.format(host=HOST, project=self.project,
                                               managedZone=self.managed_zone)

        resp = await self.http_client.get_json(url)
        rrsets = resp['rrsets']

        record_name = changes['additions'][0]['name']
        for rec in rrsets:
            if rec['name'] == record_name:
                changes['deletions'] = [rec]

        await self._publish_changes(changes, rec=True)

    def _assert_zone_for_records(self, records_to_change):
        """Assert zone for all given records.

        Args:
            records_to_change (list(dict)):
                list of records to assert their dns zone

        Raises:
            Raise GCPDropMessageError if assertion
                failed for at least on record.
        """
        for record in records_to_change:
            record_name = record['name']

            if record_name.endswith(self.dns_zone):
                continue
            else:
                msg = f'Error when asserting zone for record: {record}'
                raise exceptions.GCPDropMessageError(msg)

    @_utils.route_result
    async def publish_changes(self, event_msg):
        """Publish changes extracted from the event message.

        Args:
            event_msg (event_consumer.GEventMessage):
                contains the changes to publish.
        """
        msg_logger = _utils.GEventMessageLogger(
            self._logger, {'msg_id': event_msg.msg_id})
        msg_logger.info('Publisher received new message')

        records_to_change = event_msg.data['resourceRecords']

        self._assert_zone_for_records(records_to_change)

        action = self._get_changes_action(event_msg)

        for resource_record in records_to_change:
            changes_to_publish = self._format_changes(action,
                                                      resource_record)

            await self._publish_changes(changes_to_publish)

    def _get_changes_action(self, event_msg):
        """Get action from the event msg, raise if action is not valid.

        Args:
            event_msg (event_consumer.GEventMessage):
                event message to extract action.

        Returns:
            Return action (str) extracted from event message,
                or raise if action is not valid.
        """
        action = event_msg.data['action']

        if action == 'additions' or action == 'deletions':
            return action
        else:
            msg = ('Error trying to format changes, '
                   f'got an invalid action: {action}')
            raise exceptions.GCPDropMessageError(msg)
