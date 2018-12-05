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

__author__ = 'Lynn Root'
__version__ = '0.0.1.dev29'
__license__ = 'Apache 2.0'
__email__ = 'lynn@spotify.com'
__description__ = 'GCP Plugins for Gordon and Gordon Janitor'
__uri__ = 'https://github.com/spotify/gordon-gcp'


# Mainly for easier documentation reading
from gordon_gcp.clients import *  # noqa: F403
from gordon_gcp.exceptions import *  # noqa: F403
from gordon_gcp.plugins import *  # noqa: F403
from gordon_gcp.schema import *  # noqa: F403


__all__ = (
    clients.__all__ +  # noqa: F405
    exceptions.__all__ +  # noqa: F405
    plugins.__all__ +  # noqa: F405
    schema.__all__  # noqa: F405
)
