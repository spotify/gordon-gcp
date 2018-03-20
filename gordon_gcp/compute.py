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
Client module to enrich an event message with any missing information (
such as IP addresses to a new hostname) and to generate the desired
record(s) (e.g. ``A`` or ``CNAME`` records). Once an event message is
done (either successfully enriched or met with errors along the way),
it will be placed into the appropriate channel, either the
``success_channel`` or ``error_channel`` to be further handled by the
``gordon`` core system.

.. attention::
    The enricher client is an internal module for the core gordon
    logic. No other use cases are expected.

"""

import zope.interface
from gordon import interfaces


@zope.interface.implementer(interfaces.IEnricherClient)
class GCEEnricher:
    """Get needed instance information from Google Compute Engine.

    Args:
        config (dict): configuration relevant to Compute Engine.
        success_channel (asyncio.Queue): a sink for successfully
            processed :interface:`interfaces.IEventMessages`.
        error_channel (asyncio.Queue): a sink for
            :interface:`interfaces.IEventMessages` that were not
            processed due to problems.
    """
    phase = 'enrich'

    def __init__(self, config, success_channel, error_channel):
        self.config = config
        self.success_channel = success_channel
        self.error_channel = error_channel

    def process(self):
        # do something
        pass  # pragma: no cover
