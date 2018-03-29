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

import json
import os
import pathlib

import pytest

from gordon_gcp import schema
from gordon_gcp.schema import parse


SCHEMA_DIR = os.path.dirname(schema.__file__)
FIXTURE_DIR = pathlib.Path(SCHEMA_DIR, 'examples').absolute()
AUDIT_LOG_EXAMPLE_FILES = FIXTURE_DIR.glob('audit-log*.json')
EVENT_EXAMPLE_FILES = FIXTURE_DIR.glob('event*.json')

AUDIT_LOG_FIXTURES = [
    str(f.absolute()) for f in AUDIT_LOG_EXAMPLE_FILES
]
EVENT_FIXTURES = [
    str(f.absolute()) for f in EVENT_EXAMPLE_FILES
]


def get_expected(data, schema):
    if schema == 'audit-log':
        return {
            'action': 'additions',
            'resourceRecords': [],
            'resourceName': data['protoPayload']['resourceName'],
            'timestamp': data['timestamp'],
        }

    return {
        'action': 'additions',
        'resourceRecords': [data['resourceRecords']],
        'timestamp': data['timestamp'],
    }


@pytest.mark.parametrize('schema,fixtures', [
    ('audit-log', AUDIT_LOG_FIXTURES),
    ('event', EVENT_FIXTURES)
])
def test_parse(schema, fixtures):
    parser = parse.MessageParser()
    for fixture_file in fixtures:
        with open(fixture_file, 'r') as f:
            data = json.load(f)
        expected = get_expected(data, schema)
        actual = parser.parse(data, schema)
        assert expected == actual
