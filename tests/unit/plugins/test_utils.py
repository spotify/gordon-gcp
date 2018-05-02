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
        self.append_to_history = mocker.Mock(return_value=None)


class StubPlugin:
    phase = 'emo'

    def __init__(self, mocker):
        self._logger = logging.getLogger('')
        self.success_channel = asyncio.Queue()
        self.error_channel = asyncio.Queue()
        self.mock_update_phase = mocker.Mock(return_value=None)

    # TODO: move this up once GEventMessage "owns" this method
    async def update_phase(self, *args, **kwargs):
        event_msg = args[0]
        event_msg.phase = kwargs.get('phase') or self.phase
        self.mock_update_phase(*args, **kwargs)
        event_msg.append_to_history('updating phase')

    @_utils.route_result
    async def successful(self, event_msg):
        return None

    @_utils.route_result
    async def raises_drop_message(self, event_msg):
        raise exceptions.GCPDropMessageError('drop it')

    @_utils.route_result
    async def raises_general(self, event_msg):
        raise exceptions.GCPGordonError('give me another chance')


@pytest.mark.asyncio
@pytest.mark.parametrize('func,expected', [
    ('successful', ('emo', 1, 0, 0, None)),
    ('raises_drop_message', ('drop', 0, 1, 1,
                             ('DROPPING: Fatal exception occurred '
                              'when handling message: drop it.'))),
    # ('raises_general', (None, 0, 1, 1,
    #                     ('RETRYING: Exception occurred when handling '
    #                      'message: give me another chance.')))
])
async def test_wrap_func(func, expected, caplog, mocker):
    """event_msg is added to error channel when methods raise."""
    exp_phase, exp_succ_chnl, exp_err_chnl, exp_log_count, error_msg = expected
    plugin = StubPlugin(mocker)
    event_msg = StubEventMessage(mocker)

    func_to_test = getattr(plugin, func)

    await func_to_test(event_msg)

    assert exp_succ_chnl == plugin.success_channel.qsize()
    assert exp_err_chnl == plugin.error_channel.qsize()
    assert exp_log_count == len(caplog.records)
    assert exp_phase == event_msg.phase

    update_call = mocker.call('updating phase')
    error_call = mocker.call((error_msg, plugin.phase))
    # if error_msg:
    #     calls = [
    #         mocker.call((error_msg, plugin.phase)),
    #         mocker.call('updating phase')
    #     ]
    #     event_msg.append_to_history.assert_called_with(
    #         error_msg, plugin.phase)
    # else:
    #     event_msg.append_to_history.assert_called_once()

    if func == 'successful':
        plugin.mock_update_phase.assert_called_once_with(event_msg)
        event_msg.append_to_history.assert_called_once_with('updating phase')
    elif func == 'raises_drop_message':
        plugin.mock_update_phase.assert_called_once_with(event_msg, phase='drop')
        event_msg.append_to_history.assert_has_calls([update_call, error_call])
    else:
        plugin.mock_update_phase.assert_not_called()
        event_msg.append_to_history.assert_called_once_with(error_call)