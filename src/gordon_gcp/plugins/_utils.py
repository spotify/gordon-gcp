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
"""Common utils shared among plugins."""


import functools
import logging

from gordon_gcp import exceptions


class GEventMessageLogger(logging.LoggerAdapter):
    def process(self, log_msg, kwargs):
        log = f'[msg-{self.extra["msg_id"]}]: {log_msg}'
        return log, kwargs


def handle_errors(f):
    """Catch raised exceptions & add event_msg to the error channel."""
    @functools.wraps(f)
    async def wrapper(self, event_msg, *args, **kwargs):
        try:
            return await f(self, event_msg, *args, **kwargs)

        except Exception as e:
            msg_logger = GEventMessageLogger(
                self._logger, {'msg_id': event_msg.msg_id})

            if isinstance(e, exceptions.GCPDropMessageError):
                # update phase to dropped
                msg = f'Dropping message: {e}'
                event_msg.append_to_history(msg, self.phase)
                await self.update_phase(event_msg, phase='drop')

            else:
                msg = f'Encountered a retryable error: {e}'
                event_msg.append_to_history(msg, self.phase)

            msg_logger.warn(msg, exc_info=e)
            await self.error_channel.put(event_msg)

    return wrapper
