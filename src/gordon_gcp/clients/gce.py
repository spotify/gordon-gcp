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
Client classes to retrieve project and instance data from GCE.

These clients use the asynchronous HTTP client defined in
:class:`.AIOConnection` and require service account or JWT-token
credentials for authentication.

To use:

.. code-block:: python

    import asyncio

    import aiohttp
    import gordon_gcp

    loop = asyncio.get_event_loop()

    async def main():
        session = aiohttp.ClientSession()
        auth_client = gordon_gcp.GAuthClient(
            keyfile='/path/to/keyfile', session=session)
        client = gordon_gcp.GCEClient(auth_client, session)
        instances = await client.list_instances('project-id')
        print(instances)

    loop.run_until_complete(main())
    # example output
    # [{'hostname': 'instance-1', 'internal_ip': '10.10.10.10',
    #   'external_ip': '192.168.1.10'}]
"""

import logging

from gordon_gcp.clients import _utils
from gordon_gcp.clients import http


__all__ = ('GCEClient',)


class GCEClient(http.AIOConnection, _utils.GPaginatorMixin):
    """Async client to interact with Google Cloud Compute API.

    Attributes:
        BASE_URL (str): base compute endpoint URL.

    Args:
        auth_client (.GAuthClient): client to manage authentication for
            HTTP API requests.
        session (aiohttp.ClientSession): (optional) ``aiohttp`` HTTP
            session to use for sending requests. Defaults to the
            session object attached to :obj:`auth_client` if not
            provided.
        api_version (str): version of API endpoint to send requests to.
        blacklisted_tags (list): Do not collect an instance if it has
            been tagged with any of these.
        blacklisted_metadata (list): Do not collect an instance if its
            metadata key:val matches a {key:val} dict in this list.
    """
    BASE_URL = 'https://www.googleapis.com/compute/'

    def __init__(self,
                 auth_client=None,
                 session=None,
                 api_version='v1',
                 blacklisted_tags=None,
                 blacklisted_metadata=None):
        super().__init__(auth_client=auth_client, session=session)
        self.api_version = api_version
        self.blacklisted_tags = blacklisted_tags or []
        self.blacklisted_metadata = blacklisted_metadata or []

    async def list_instances(self,
                             project,
                             page_size=500,
                             instance_filter=None):
        """Fetch all instances in a GCE project.

        You can find the endpoint documentation `here <https://cloud.
        google.com/compute/docs/reference/latest/instances/
        aggregatedList>`__.

        Args:
            project (str): unique, user-provided project ID.
            page_size (int): hint for the client to only retrieve up to
                this number of results per API call.
            instance_filter (str): endpoint-specific filter string used
                to retrieve a subset of instances. This is passed
                directly to the endpoint's "filter" URL query parameter.
        Returns:
            list(dicts): data of all instances in the given
                :obj:`project`
        """
        url = (f'{self.BASE_URL}{self.api_version}/projects/{project}'
               '/aggregated/instances')
        params = {'maxResults': page_size}
        if instance_filter:
            params['filter'] = instance_filter

        responses = await self.list_all(url, params)
        instances = self._parse_rsps_for_instances(responses)
        return instances

    def _parse_rsps_for_instances(self, responses):
        instances = []
        for response in responses:
            for zone in response.get('items', {}).values():
                instances.extend(self._filter_zone_instances(zone))
        return instances

    def _filter_zone_instances(self, zone):
        instances = []
        for instance in zone.get('instances', []):
            if not any([
                self._blacklisted_by_tag(instance),
                self._blacklisted_by_metadata(instance)
            ]):
                instances.append(instance)
        return instances

    def _blacklisted_by_tag(self, instance):
        instance_tags = instance.get('tags', {}).get('items', [])
        for tag in instance_tags:
            if tag in self.blacklisted_tags:
                msg = (f'Instance "{instance["name"]}" filtered out for '
                       f'blacklisted tag: "{tag}"')
                logging.debug(msg)
                return True
        return False

    def _blacklisted_by_metadata(self, instance):
        # NOTE: Both key and value are used when comparing the instance and
        # blacklist metadata.
        instance_metadata = instance.get('metadata', {}).get('items', [])
        for metadata in instance_metadata:
            for bl_meta in self.blacklisted_metadata:
                if bl_meta.get(metadata['key']) == metadata['value']:
                    msg = (f'Instance "{instance["name"]}" filtered out for '
                           f'blacklisted metadata: "{bl_meta}"')
                    logging.debug(msg)
                    return True
        return False
