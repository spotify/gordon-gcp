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

import aiohttp
import zope.interface
from gordon import interfaces

from gordon_gcp.plugins import _utils, GEventMessage
from gordon_gcp import exceptions
from gordon_gcp.clients import http, auth

__all__ = ('GDNSPublisher',)

HOST = 'https://www.googleapis.com'
BASE_CHANGES_ENDPOINT = '/dns/{version}/projects/{project}/managedZones/{managedZone}/changes'
RRSETS_ENDPOINT = '/dns/v1/projects/{project}/managedZones/{managedZone}/rrsets'


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
        _api_version = config.get('version', 'v1')
        self.base_url = HOST + \
                        BASE_CHANGES_ENDPOINT.format(version=_api_version,
                                                     project=self.project,
                                                     managedZone=self.managed_zone)
        self._logger = logging.getLogger('')

    # TODO: This will be eventually moved to GEventMessage
    async def update_phase(self, event_msg, phase=None):
        old_phase = event_msg.phase
        event_msg.phase = phase or self.phase
        msg = f'Updated phase from "{old_phase}" to "{event_msg.phase}".'
        event_msg.append_to_history(msg, self.phase)

    def _format_change(self, resource_record):
        """Returns ResourceRecordSets to change.

        Args:
            resource_record (dict): dict contains record data
        Returns:
            ResourceRecordSets to pass
                to the API to make changes.
        """
        return {
                'kind': 'dns#resourceRecordSet',
                'name': resource_record['name'],
                'type': resource_record['type'],
                'ttl': resource_record.get('ttl', self.config.get('default_ttl', 300)),
                'rrdatas': resource_record['rrdatas']
        }

    def _format_changes(self, action, resource_record):
        """Return dict containing the changes
            to be made.

        Args:
            action (str): change action to add
                to record changes dict

        Returns:
            The changes to post to google API.
        """
        record_changes = self._format_change(resource_record)

        return {
            'kind': 'dns#change',
            action: [record_changes]
        }

    async def _watch_status(self, changes_id):
        """Check google API if the changes are done.

        Args:
            changes_id (int): check the status for the changes_id

        Returns:
            True if the changes are done,
             or raise error if timeout passed and
             we stiLl didn't get status "done" from the API
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
        """Post changes to the google API and sample it to
            check if the changes are done.

        Args:
            changes (dict): the changes to make
            rec (bool): flag to call _handle_update only once.

        Returns:
            boolean if the changes are done.
        """
        try:
            print('CHANGES', changes)
            resp = await self.http_client.request('post', self.base_url,
                                                  json=changes)
        except Exception as e:
            e_start = 'Issue connecting to www.googleapis.com: '
            print('ACTUAL E: ', e.args[0])
            status_code = int(e.args[0].split(e_start)[1].split(',')[0])
            if status_code == 409 and not rec:
                await self._handle_update(changes)
                return
            elif 400 <= status_code < 500:
                msg = f'Error: {e} for changes: {changes}'
                raise exceptions.GCPDropMessageError(msg)
            else:
                raise e

        resp_dict = json.loads(resp)

        # TODO: create another task to measure propagation time
        return await self._watch_status(resp_dict['id'])

    async def _handle_update(self, changes):
        """Handle update changes, after getting 409 error,
            get the existing record and append deletion
            action with the rec to the changes,
            then publish changes again.

        Args:
            changes: add the deletions action of
                the existing record to the changes

        """
        url = HOST + RRSETS_ENDPOINT.format(project=self.project,
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
            records_to_change (list):
                records to assert their dns zone

        Returns:
            Raise GCPDropMessageError if assertion
             failed for at least on record.
        """
        for record in records_to_change:
            record_name = record['name']

            if record_name.endswith(self.dns_zone):
                continue
            else:
                msg = f'Error when asserting zone' \
                      f' for record: {record}'
                raise exceptions.GCPDropMessageError(msg)

    @_utils.route_result
    async def publish_changes(self, event_msg):
        """Publish changes extracted from the event message,
            place the msg into the appropriate channel.

        Args:
            event_msg (event_consumer.GEventMessage):
                contains the changes to publish
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
        """Return action extracted from event message,
            or raise if action is not valid.

        Args:
            event_msg: message to extract action

        Returns:
            Return action extracted from event message,
            or raise if action is not valid.
        """
        action = event_msg.data['action']

        if action == 'additions' or action == 'deletions':
            return action
        else:
            msg = f'Error trying to format changes, ' \
                  f'got an invalid action: {action}'
            raise exceptions.GCPDropMessageError(msg)


class PubSubMessage(object):
    def __init__(self):
        self.message_id = 123


if __name__ == '__main__':
    kwargs = {
        'keyfile': "/Users/nuriti/src/gordon-gcp/src/gordon_gcp/plugins/pr-tower-hackweek-1393cea578e8.json",
        'scopes': ['cloud-platform'],
    }
    auth_client = auth.GAuthClient(**kwargs)

    http_client = http.AIOConnection(auth_client=auth_client)
    config = {'timeout': 90,
              'managed_zone': 'nurit-com',
              'project': 'pr-tower-hackweek',
              'api_version': 'v1',
              'dns_zone': 'nurit.com.'}
    success, error = asyncio.Queue(), asyncio.Queue()
    publisher = GDNSPublisher(config, success, error, http_client)

    loop = asyncio.get_event_loop()

    event_msg_data = {
        'action': 'additions',
        'resourceName': 'projects/.../instances/an-instance-name-b45c',
        'resourceRecords': [
            {
                'name': 'service.nurit.com.',
                'rrdatas': ['127.10.20.22', '127.10.20.25'],
                'type': 'A',
                'ttl': 3600
            }
        ]
    }
    pubsub_msg = PubSubMessage()
    event_msg = GEventMessage(pubsub_msg, event_msg_data)

    loop.run_until_complete(publisher.publish_changes(event_msg))
