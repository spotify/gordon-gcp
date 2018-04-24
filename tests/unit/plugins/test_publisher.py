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

import asyncio
import aiohttp
import pytest  # NOQA
from gordon import interfaces

from gordon_gcp.plugins import publisher
from gordon_gcp.clients import auth
from gordon_gcp.clients import http


@pytest.mark.parametrize('provide_session', [True, False])
def test_implements_interface(provide_session, mocker):
    """GDNSPublisher implements IPublisherClient"""

    session = None
    if provide_session:
        session = aiohttp.ClientSession()

    auth_client = mocker.Mock(auth.GAuthClient)
    auth_client._session = aiohttp.ClientSession()
    creds = mocker.Mock()
    auth_client.creds = creds
    client = http.AIOConnection(auth_client=auth_client, session=session)

    config = {'foo': 'bar'}
    success, error = asyncio.Queue(), asyncio.Queue()
    client = publisher.GDNSPublisher(config, success, error, client)

    assert interfaces.IPublisherClient.providedBy(client)
    assert interfaces.IPublisherClient.implementedBy(publisher.GDNSPublisher)
    assert config is client.config
    assert success is client.success_channel
    assert error is client.error_channel
    assert 'publish' == client.phase


