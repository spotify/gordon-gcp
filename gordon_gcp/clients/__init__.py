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

# Mainly for easier documentation reading
from gordon_gcp.clients.auth import *  # noqa: F403
from gordon_gcp.clients.http import *  # noqa: F403


__all__ = (
    auth.__all__ +  # noqa: F405
    http.__all__  # noqa: F405
)
