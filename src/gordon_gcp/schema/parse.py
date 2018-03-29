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
"""
Module to parse loaded JSON messages according to GCP-related schemas
and return only the actionable data.

To use:

.. code-block:: pycon

    >>> import json
    >>> from gordon_gcp.schema import parse
    >>> parser = parse.MessageParser()
    >>> # using an example file
    >>> exp = 'gordon_gcp/schema/examples/audit-log.last-operation.json'
    >>> with open(exp, 'r') as f:
    ...   data = json.load(f)
    >>> message = parser.parse(data, 'audit-log')
    >>> message
    {
        'action': 'additions',
        'resourceName': 'projects/.../instances/an-instance-name-b45c',
        'resourceRecords': []
    }

"""


__all__ = ('MessageParser',)


class MessageParser:
    """Parse a message provided a given GCP schema."""
    ACTION_MAPPER = {
        'v1.compute.instances.insert': 'additions',
        'v1.compute.instances.delete': 'deletions',
    }

    def _parse_audit_log_msg(self, message):
        # TODO (lynn): what about first vs last messages?
        payload = message['protoPayload']
        data = {
            'action': self.ACTION_MAPPER[payload['methodName']],
            'resourceName': payload['resourceName'],
            'resourceRecords': [],
            'timestamp': message['timestamp'],
        }
        return data

    def _parse_event_msg(self, message):
        # TODO (lynn): potentially should update schema to require a
        #              list of records rather than shoehorning it here
        message['resourceRecords'] = [message['resourceRecords']]
        return message

    def parse(self, message, schema):
        """Parse message according to schema.

        `message` should already be validated against the given schema.
        See :ref:`schemadef` for more information.

        Args:
            message (dict): message data to parse.
            schema (str): valid message schema.
        Returns:
            (dict): parsed message
        """
        func = {
            'audit-log': self._parse_audit_log_msg,
            'event': self._parse_event_msg,
        }[schema]
        return func(message)
