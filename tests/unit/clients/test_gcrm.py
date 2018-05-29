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

import copy

import pytest
from aioresponses import aioresponses

from gordon_gcp.clients import gcrm


class TestGCRMClient:
    @pytest.fixture
    def patch_crm_url(self, monkeypatch):
        fake_url = 'https://example.com'
        class_attribute = 'gordon_gcp.clients.gcrm.GCRMClient.BASE_URL'
        monkeypatch.setattr(class_attribute, fake_url)
        return fake_url

    @pytest.fixture
    def crm_one_page_rsp(self):
        return {
            'projects': [
                {
                    'projectNumber': '1',
                    'projectId': 'project-service-1',
                    'lifecycleState': 'ACTIVE',
                    'name': 'Project Service 1',
                    'createTime': '2018-01-01T00:00:00',
                    'labels': {},
                    'parent': {}
                },
                {
                    'projectNumber': '2',
                    'projectId': 'project-service-2',
                    'lifecycleState': 'ACTIVE',
                    'name': 'Project Service 2',
                    'createTime': '2018-02-01T00:00:00',
                    'labels': {},
                    'parent': {}
                }],
        }

    @pytest.mark.asyncio
    async def test_list_all_active_projects(
            self, mocker, crm_one_page_rsp, patch_crm_url, get_gce_client):
        """Request is made using default parameters."""
        crm_client = get_gce_client(gcrm.GCRMClient)

        api_url = f'{patch_crm_url}/v1/projects?'
        with aioresponses() as m:
            m.get(f'{api_url}pageSize=500', payload=crm_one_page_rsp)

            results = await crm_client.list_all_active_projects(
                page_size=500)

        assert crm_one_page_rsp['projects'] == results

    @pytest.mark.asyncio
    async def test_list_all_active_projects_multiple_pages(
            self, mocker, crm_one_page_rsp, patch_crm_url, get_gce_client):
        """Client successfully retrieves multiple pages of results from API."""
        crm_client = get_gce_client(gcrm.GCRMClient)
        page2 = copy.deepcopy(crm_one_page_rsp)
        page2['projects'][0]['projectNumber'] = '3'
        page2['projects'][1]['projectNumber'] = '4'
        # This project entry is expected to be filtered out
        page2['projects'][1]['lifecycleState'] = 'd34d'
        crm_one_page_rsp['nextPageToken'] = '123token'

        api_url = f'{patch_crm_url}/v1/projects?'
        with aioresponses() as m:
            url_with_pagesize = f'{api_url}pageSize=1000'
            m.get(url_with_pagesize, payload=crm_one_page_rsp)
            url_with_token = f'{api_url}pageSize=1000&pageToken=123token'
            m.get(url_with_token, payload=page2)

            results = await crm_client.list_all_active_projects()

        expected_rsp = crm_one_page_rsp['projects']
        expected_rsp.append(page2['projects'].pop(0))
        assert expected_rsp == results
        # assert requests made and their sequence
        requests = list(m.requests.keys())
        assert 2 == len(requests)
        assert ('get', url_with_pagesize) == requests[0]
        assert ('get', url_with_token) == requests[1]
