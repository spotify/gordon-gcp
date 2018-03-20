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

import pytest  # NOQA
from gordon import interfaces

from gordon_gcp import dns


def test_implements_interface():
    """GDNSPublisher implements IPublisherClient"""
    config = {'foo': 'bar'}
    success, error = asyncio.Queue(), asyncio.Queue()
    client = dns.GDNSPublisher(config, success, error)

    assert interfaces.IPublisherClient.providedBy(client)
    assert interfaces.IPublisherClient.implementedBy(dns.GDNSPublisher)
    assert config is client.config
    assert success is client.success_channel
    assert error is client.error_channel
    assert 'publish' == client.phase
