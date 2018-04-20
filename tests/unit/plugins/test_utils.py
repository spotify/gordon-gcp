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

import asyncio
import logging

import pytest

from gordon_gcp import exceptions
from gordon_gcp.plugins import _utils




class StubEventMessage:
    def __init__(self, mocker):
        self.msg_id = '1234'
        self.phase = None
        self.append_to_history = mocker.Mock(return_value=True)

    # TODO: attach a mock to assert called
    # def append_to_history(*args, **kwargs):
    #     return True


class StubPlugin:
    phase = 'emo'

    def __init__(self, mocker):
        self._logger = logging.getLogger('')
        self.error_channel = asyncio.Queue()

    # TODO: attach a mock to assert called
    async def update_phase(*args, **kwargs):
        return True

    @_utils.handle_errors
    async def successful(self, event_msg):
        return True

    @_utils.handle_errors
    async def raises_drop_message(self, event_msg):
        raise exceptions.GCPDropMessageError('drop it!')

    @_utils.handle_errors
    async def raises_general(self, event_msg):
        raise exceptions.GCPGordonError('give me another chance!')


@pytest.mark.asyncio
@pytest.mark.parametrize('func,exp_chnl_size,error_msg', [
    ('successful', 0, None),
    ('raises_drop_message', 1, 'Dropping message: drop it!'),
    ('raises_general', 1,
     'Encountered a retryable error: give me another chance!')
])
async def test_wrap_func(func, exp_chnl_size, error_msg, mocker):
    """event_msg is added to error channel when methods raise."""
    plugin = StubPlugin(mocker)
    event_msg = StubEventMessage(mocker)

    func_to_test = getattr(plugin, func)

    await func_to_test(event_msg)

    assert exp_chnl_size == plugin.error_channel.qsize()

    if error_msg:
        event_msg.append_to_history.assert_called_once_with(
            error_msg, plugin.phase)

    # TODO: see L46 re: mock async method for update_phase
    # if func == 'raises_drop_message':
    #     plugin.update_phase.assert_called_once_with(event_msg, phase='drop')
