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
"""
A GCEAuthority retrieves a list of all instances in all projects that it has
access to, and which belong to the configured zone. For every project, it will
create a message containing domain record information and put it into the
rrset channel. Projects can be filtered by 'project name'. Instances can be
filtered by tags and metadata.

To use:

.. code-block:: python

    import asyncio

    from gordon_gcp.plugins import janitor

    async def run():
        rrset_channel = asyncio.queue()
        authority = janitor.get_authority(config, rrset_channel)
        await authority.start()
        msg = await rrset_channel.get()
        print(msg)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    # prints: {'zone': 'example.com', 'resourceRecords': [...]}
"""

import asyncio
import logging

import aiohttp
import zope.interface
from gordon_janitor import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.clients import gce
from gordon_gcp.clients import gcrm


__all__ = ('GCEAuthority', 'GCEAuthorityBuilder')


class GCEAuthorityBuilder:
    """Build and configure a :class:`GCEAuthority` object.

    Args:
        config (dict): plugin-specific configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        rrset_channel (asyncio.Queue): channel to send resource record messages
            to.
    """
    def __init__(self, config, metrics, rrset_channel, **kwargs):
        self.config = config
        self.metrics = metrics
        self.rrset_channel = rrset_channel
        self.kwargs = kwargs
        self.session = None

    def _get_crm_client(self, keyfile_path, scopes):
        crm_auth = auth.GAuthClient(
            keyfile_path, scopes=scopes, session=self.session)
        return gcrm.GCRMClient(crm_auth, self.session)

    def _get_gce_client(self, keyfile_path, scopes):
        tag_blacklist = self.config.get('tag_blacklist', [])
        _metadata_blacklist = self.config.get('metadata_blacklist', [])
        metadata_blacklist = [dict([pair]) for pair in _metadata_blacklist]

        gce_auth = auth.GAuthClient(keyfile_path, scopes=scopes,
                                    session=self.session)
        return gce.GCEClient(gce_auth, self.session,
                             blacklisted_tags=tag_blacklist,
                             blacklisted_metadata=metadata_blacklist)

    def _validate_config(self):
        if not self.config.get('keyfile'):
            msg = ('The path to a Service Account JSON keyfile is required to '
                   'authenticate to Google Compute Engine and Cloud '
                   'Resource Manager.')
            logging.error(msg)
            raise exceptions.GCPConfigError(msg)
        if not self.config.get('dns_zone'):
            msg = ('The absolute DNS zone, i.e. "example.com.", is required to '
                   'identify to which zone generated records should belong.')
            logging.error(msg)
            raise exceptions.GCPConfigError(msg)

    def build_authority(self):
        self._validate_config()
        keyfile_path = self.config['keyfile']
        scopes = self.config.get('scopes')
        self.session = aiohttp.ClientSession()
        crm_client = self._get_crm_client(keyfile_path, scopes)
        gce_client = self._get_gce_client(keyfile_path, scopes)

        return GCEAuthority(self.config, self.metrics, crm_client, gce_client,
                            self.rrset_channel, **self.kwargs)


@zope.interface.implementer(interfaces.IAuthority)
class GCEAuthority:
    """Gather instance data from GCE.

    Args:
        config (dict): plugin-specific configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        crm_client (.GCRMClient): client used to fetch GCE projects.
        gce_client (.GCEClient): client used to fetch instances for a project.
        rrset_channel (asyncio.Queue): channel to send resource record messages
            to.
    """

    PROJECT_SKIP_RESP_CODES = {403, 404, 410}

    def __init__(self, config, metrics, crm_client, gce_client,
                 rrset_channel=None, **kwargs):
        self.config = config
        self.metrics = metrics
        self.crm_client = crm_client
        self.gce_client = gce_client
        self.rrset_channel = rrset_channel

    async def _get_active_project_ids(self):
        active_projects = await self.crm_client.list_all_active_projects()
        return set(p.get('projectId') for p in active_projects)

    async def _get_projects(self):
        projects = await self._get_active_project_ids()
        project_blacklist = set(self.config.get('project_blacklist', []))
        sorted_projects = sorted(projects - project_blacklist)
        context = {'plugin': 'gceauthority'}
        await self.metrics.set(
            'projects', len(sorted_projects), context=context)
        return sorted_projects

    def _filter_results(self, results):
        successful_results = []
        for index, result in enumerate(results):
            if (isinstance(result, exceptions.GCPHTTPResponseError) and
                    result.status in GCEAuthority.PROJECT_SKIP_RESP_CODES):
                msg = (f'Could not fetch instance list for project, skipping: '
                       f'{result}')
                logging.warn(msg)
            elif isinstance(result, Exception):
                raise result
            else:
                successful_results.extend(result)

        return successful_results

    async def _get_instances(self, projects):
        instance_filter = self.config.get('instance_filter')

        coros = set()
        for project in projects:
            coro = self.gce_client.list_instances(
                project, instance_filter=instance_filter)
            coros.add(coro)

        all_results = await asyncio.gather(*coros, return_exceptions=True)

        return self._filter_results(all_results)

    def _create_instance_rrset(self, instance):
        ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        fqdn = f"{instance['name']}.{self.config['dns_zone']}"
        return {
            'name': fqdn,
            'type': 'A',
            'rrdatas': [ip]
        }

    def _create_msgs(self, instances):
        msgs = []
        rrsets = []
        for instance in instances:
            try:
                rrsets.append(self._create_instance_rrset(instance))
            except (KeyError, IndexError) as e:
                instance_name = instance.get('name')
                logging.warn(
                    'Could not extract instance information for '
                    f'{instance_name} because of missing key {e}, skipping.')
        if rrsets:
            msgs.append({
                'zone': self.config['dns_zone'],
                'rrsets': rrsets
            })

        return msgs

    async def run(self):
        """Batch instance data and send it to the :obj:`self.rrset_channel`.
        """
        plugin = 'gceauthority'
        timer_context = {'plugin': plugin}
        timer = self.metrics.timer(
            'plugin-runtime', context=timer_context)
        await timer.start()
        projects = await self._get_projects()
        instances = await self._get_instances(projects)

        for rrset_msg in self._create_msgs(instances):
            zone = rrset_msg['zone']
            rrsets = rrset_msg['rrsets']
            total_rrsets = len(rrsets)
            msg = f'[{zone}] Found {total_rrsets} rrsets for zone in GCE.'
            logging.info(msg)
            context = {'plugin': plugin,
                       'zone': zone}
            await self.metrics.set('rrsets',
                                   total_rrsets, context=context)
            await self.rrset_channel.put(rrset_msg)

        await self.cleanup()
        await timer.stop()

    async def cleanup(self):
        """Clean up after a run."""
        msg = 'Finished processing GCE data.'
        logging.info(msg)
        await self.rrset_channel.put(None)
        await self.gce_client._session.close()
