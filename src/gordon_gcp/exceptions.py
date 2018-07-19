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

from gordon import exceptions as core_exceptions


__all__ = (
    'GCPGordonError', 'GCPGordonJanitorError', 'InvalidMessageError',
    'InvalidDNSZoneInMessageError', 'GCPHTTPError', 'GCPHTTPResponseError',
    'GCPAuthError', 'GCPConfigError', 'GCPPublishRecordTimeoutError'
)


class GCPGordonError(core_exceptions.GordonError):
    """General Gordon GCP Plugin Error."""


class GCPGordonJanitorError(core_exceptions.GordonError):
    """General Gordon GCP Janitor Plugin Error."""


class InvalidMessageError(GCPGordonError):
    """Consumed an invalid message from Google Pub/Sub."""


class InvalidDNSZoneInMessageError(GCPGordonError):
    """Raised when a message with an invalid DNS zone is consumed."""


class GCPHTTPError(GCPGordonError):
    """An error occurred while processing an HTTP request."""


class GCPHTTPResponseError(GCPHTTPError):
    """An HTTP response had an error associated with a status code."""
    def __init__(self, message, status):
        super().__init__(message)
        self.status = status


class GCPAuthError(GCPGordonError):
    """Authentication error with Google Cloud."""


class GCPConfigError(GCPGordonError):
    """Improper or incomplete configuration for plugin."""


class GCPPublishRecordTimeoutError(GCPGordonError):
    """Time out error when attempting to publish records."""
