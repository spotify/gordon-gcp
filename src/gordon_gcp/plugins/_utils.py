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


# TODO: should this be named something like `handle_errors_async`?
def route_result(f):
    """Route ``event_msg`` to respective result channel.

    If the decorated method returns a successful result, the
    ``event_msg``'s phase is updated and is added to the
    :obj:`self.success_channel`.

    If the decorated method raises :exc:`exceptions.GCPDropMessageError`
    then the ``event_msg``'s phase is updated to `drop` and added to the
    :obj:`self.error_channel` to be dropped.

    If the decorated method raises any other type of exception, then the
    ``event_msg` is added to the :obj:`self.error_channel` to be retried.

    .. attention::
        Must only be used to decorate asynchronous methods.

    Args:
        event_msg (.GEventMessage): message with changes to publish.
            NOTE: This must be the first argument provided in the
            decorated method, or otherwise passed in via keyword.
        args (list): any additional arguments for the decorated method.
        kwargs (dict): any additional keyword arguments for the
            decorated method.
    """
    @functools.wraps(f)
    async def wrapper(self, event_msg, *args, **kwargs):
        try:
            ret = await f(self, event_msg, *args, **kwargs)
            await self.update_phase(event_msg)
            await self.success_channel.put(event_msg)
            return ret  # may be unneeded, but just to be safe

        except Exception as e:
            msg_logger = GEventMessageLogger(
                self._logger, {'msg_id': event_msg.msg_id})

            if isinstance(e, exceptions.GCPDropMessageError):
                # update phase to dropped
                # Q: is there a better wording for this message?
                msg = (f'DROPPING: Fatal exception occurred when handling '
                       f'message: {e}.')
                event_msg.append_to_history(msg, self.phase)
                await self.update_phase(event_msg, phase='drop')

            else:
                # TODO: potentially add to `msg` the number of retries left
                #       or the number of current retries once core retry
                #       logic is implemented
                # Q: is there a better wording for this message?
                msg = ('RETRYING: Exception occurred when handling message: '
                       f'{e}.')
                event_msg.append_to_history(msg, self.phase)

            msg_logger.warn(msg, exc_info=e)
            await self.error_channel.put(event_msg)

    return wrapper
