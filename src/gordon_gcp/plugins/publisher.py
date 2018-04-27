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

    def _format_change(self, event_msg):
        """Returns record data from the event message to change.

        Args:
            event_msg (event_consumer.GEventMessage):
                contains the record data to add\delete

        Returns:
            record data extracted from the event message
        """
        changes = []
        resource_records = event_msg.data['resourceRecords']

        for resource_record in resource_records:
            changes.append({
                'kind': 'dns#resourceRecordSet',
                'name': resource_record['name'],
                'type': resource_record['type'],
                'ttl': resource_record.get('ttl', self.config.get('default_ttl', 300)),
                'rrdatas': resource_record['rrdatas']
            })

        return changes

    def _format_changes(self, event_msg):
        """Return dict containing the changes
            to be made to the zone.

        Args:
            event_msg (event_consumer.GEventMessage):
                contains the change action to make

        Returns:
            The changes to post to google API.
        """
        changes = self._format_change(event_msg)

        ret = {
            'kind': 'dns#change'
        }

        action = event_msg.data['action']
        if action == 'additions':
            ret['additions'] = changes
        elif action == 'deletions':
            ret['deletions'] = changes
        else:
            msg = f'Error trying to format changes, ' \
                  f'got an invalid action: {action}'
            raise exceptions.GCPDropMessageError(msg)

        return ret

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

    async def _publish_changes(self, changes):
        """Post changes to the google API and sample it to
            check if the changes are done.

        Args:
            changes (dict): the changes to make

        Returns:
            boolean if the changes are done.
        """
        try:
            resp = await self.http_client.request('post', self.base_url,
                                                  json=changes)
        except Exception as e:
            e_start = 'Issue connecting to www.googleapis.com: '
            status_code = int(e.args[0].split(e_start)[1].split(',')[0])
            if 400 <= status_code < 500:
                raise exceptions.GCPDropMessageError(e)
            else:
                raise e

        resp_dict = json.loads(resp)

        # TODO: create another task to measure propagation time
        return await self._watch_status(resp_dict['id'])

    def _find_zone(self, event_msg):
        """Find the zone to make changes
            to from the record name, according to the valid zones.

        Args:
            event_msg (event_consumer.GEventMessage):
                contains the record to find the zone

        Returns:
            The zone to make changes to or
                raise if zone not found.
        """
        record = event_msg.data['resourceRecords'][0]
        record_name = record['name']

        if record_name.endswith(self.dns_zone):
            return self.managed_zone

        msg = f'Error trying to find zone' \
              f' in valid_zone for record: {record}'
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

        self._find_zone(event_msg)

        changes_to_publish = self._format_changes(event_msg)

        await self._publish_changes(changes_to_publish)


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
                'rrdatas': ['127.10.20.22'],
                'type': 'A',
                'ttl': 3600
            },
            {
                'name': 'service50.nurit.com.',
                'rrdatas': ['127.10.20.1000'],
                'type': 'A',
                'ttl': 3600
            }
        ]
    }
    pubsub_msg = PubSubMessage()
    event_msg = GEventMessage(pubsub_msg, event_msg_data)

    loop.run_until_complete(publisher.publish_changes(event_msg))
