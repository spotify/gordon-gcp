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

from gordon_gcp.clients import _utils
from gordon_gcp.clients import http


__all__ = ('GCRMClient',)


class GCRMClient(http.AIOConnection, _utils.GPaginatorMixin):
    """Async client to interact with Google Cloud Resource Manager API.

    You can find the endpoint documentation `here <https://cloud.google.
    com/resource-manager/reference/rest/#rest-resource-v1projects>`__.

    Attributes:
        BASE_URL (str): Base endpoint URL.

    Args:
        auth_client (.GAuthClient): client to manage authentication for
            HTTP API requests.
        session (aiohttp.ClientSession): (optional) ``aiohttp`` HTTP
            session to use for sending requests. Defaults to the
            session object attached to :obj:`auth_client` if not provided.
        api_version (str): version of API endpoint to send requests to.
    """
    BASE_URL = 'https://cloudresourcemanager.googleapis.com'

    def __init__(self, auth_client=None, session=None, api_version='v1'):
        super().__init__(auth_client=auth_client, session=session)
        self.api_version = api_version

    def _parse_rsps_for_projects(self, responses):
        projects = []
        for response in responses:
            for project in response.get('projects', []):
                projects.append(project)
        return projects

    async def list_all_active_projects(self, page_size=1000):
        """Get all active projects.

        You can find the endpoint documentation `here <https://cloud.
        google.com/resource-manager/reference/rest/v1/projects/list>`__.

        Args:
            page_size (int): hint for the client to only retrieve up to
                this number of results per API call.
        Returns:
            list(dicts): all active projects
        """
        url = f'{self.BASE_URL}/{self.api_version}/projects'
        params = {'pageSize': page_size}

        responses = await self.list_all(url, params)
        projects = self._parse_rsps_for_projects(responses)
        return [
            project for project in projects
            if project.get('lifecycleState', '').lower() == 'active'
        ]
