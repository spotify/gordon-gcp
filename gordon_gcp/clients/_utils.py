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
"""Common utils shared among clients."""

import platform


PY_VERSION = platform.python_version()
DEFAULT_REQUEST_HEADERS = {
    'X-Goog-API-Client': f'custom-aiohttp-gcloud-python/{PY_VERSION} gccl',
    'Accept-Encoding': 'gzip',
    'User-Agent': 'custom-aiohttp-gcloud-python',
}

# aiohttp does not log client request/responses; mimicking
# `requests` log format
REQ_LOG_FMT = 'Request: "{method} {url}"'
RESP_LOG_FMT = 'Response: "{method} {url}" {status} {reason}'
