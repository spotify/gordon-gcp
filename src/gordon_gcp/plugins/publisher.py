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

import zope.interface
from gordon import interfaces

from gordon_gcp.plugins import _utils


__all__ = ('GDNSPublisher',)


HOST = 'https://www.googleapis.com'
V1BETA2 = '/dns/v1beta2/projects/{project}/managedZones/{managedZone}/changes'
V1 = '/dns/v1/projects/{project}/managedZones/{managedZone}/changes'


# TODO: should we use the v1beta2 endpoint? it has a boolean for
#       changes API - `isServing` - that maybe we should make use of, at
#       least for testing?
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
    """
    phase = 'publish'

    def __init__(self, config, success_channel, error_channel, http_client):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel
        self.http_client = http_client
        self.timeout = config['timeout']
        self.valid_zones = config['valid_zones']
        self._logger = logging.getLogger('')

    # TODO: This will be eventually moved to GEventMessage
    async def update_phase(self, phase=None):
        old_phase = event_msg.phase
        event_msg.phase = phase or self.phase
        msg = f'Updated phase from "{old_phase}" to "{event_msg.phase}".'
        event_msg.append_to_history(msg, self.phase)

    # TODO: once change has been published, regularly pull the changes
    #       API to assert done - with timeout I assume
    def _format_change(self, event_msg):
        # NOTE: https://cloud.google.com/dns/api/v1/resourceRecordSets#resource
        data = event_msg.data['resourceRecords']
        return {
            'kind': 'dns#resourceRecordSet',
            'name': data['name'],
            'type': data['type'],
            'ttl': data.get('ttl', config.get('default_ttl', 300)),
            'rrdatas': data['rrdatas']
        }

    def _format_changes(self, event_msg):
        changes = self._format_change(event_msg)

        datefmt = '%Y-%m-%dT%H:%M:%S.%fZ%z'  # 2018-03-27T13:29:12.623222Z
        now = datetime.datetime.utcnow()
        ret = {
            'kind': 'dns#change',
            # TODO: not sure if this is allowed, or is only in response
            'startTime': now.strftime(datefmt),
        }
        if event_msg.data['action'] == 'additions':
            ret['additions'] = [changes]
        elif event_msg.data['action'] == 'deletions':
            ret['deletions'] = [changes]
        else:
            # Q: will this ever happen?
            raise Exception('not a valid action')

    async def _watch_status(self, zone, changes_id):
        url = V1.format(project=self.project, managedZone=zone)
        url = f'{url}/{changes_id}'

        timeout = datetime.timedelta(seconds=self.timeout)
        start = datetime.datetime.now()
        end = start + timeout

        # TODO: is there a better way to do this?
        while datetime.datetime.now() < end:
            resp = await self.http_client.get_json(url)
            if resp['status'] == 'done':
                return True
            await asyncio.sleep(1)

        # TODO: maybe better exception message?
        msg = 'Timed out waiting for DNS changes to update.'
        raise exceptions.GCPRetryMessageError(msg)

    async def _publish_changes(self, zone, changes):
        url = V1.format(project=self.project, managedZone=zone)
        resp = await self.http_client.request('post', url, json=changes)

        # TODO: create another task to measure propagation time
        return await _watch_status(zone, resp['id'])

    def _find_zone(self, event_msg):
        record_name = event_msg.data['resourceRecordSet']['name']
        for zone in self.valid_zones:
            # assert zone has a trailing dot
            if record_name.endswith(zone):
                return zone

    @_utils.handle_errors
    async def publish_changes(self, event_msg):
        # drop msg if not successful
        zone = self._find_zone(event_msg)
        # drop msg if not successful
        changes = self._format_changes(event_msg)
        # fail/retry if not successful
        self._publish_changes(zone, changes)

        await self.update_phase(event_msg)
        await self.success_channel.put(event_msg)
