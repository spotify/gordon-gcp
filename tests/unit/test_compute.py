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
from gordon.interfaces.reference_client import IResourceReferenceClient

import pytest  # NOQA

from gordon_gcp import ComputeEngineClient


def test_implements_interface():
    """ComputeEngineClient provides IResourceReferenceClient"""
    config = {'foo': 'bar'}
    client = ComputeEngineClient(config)
    assert issubclass(ComputeEngineClient, IResourceReferenceClient)
    assert isinstance(client, IResourceReferenceClient)
    assert config == client.config
    assert 'Google Compute Engine' == client.name
