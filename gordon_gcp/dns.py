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
Module to implement the ``IDNSProviderClient`` interface defined in
`gordon-dns <https://github.com/spotify/gordon>`_ .
"""

from gordon.interfaces.dns_client import IDNSProviderClient


class CloudDNSClient(IDNSProviderClient):
    """IDNSProviderClient provider for Google Cloud DNS.

    Args:
        config (dict): configuration relevant to Cloud DNS.
    """
    name = 'Google Cloud DNS'

    def __init__(self, config):
        self.config = config

    def run(self):
        # do something
        pass  # pragma: no cover
