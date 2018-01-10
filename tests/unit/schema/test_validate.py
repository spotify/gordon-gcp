# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 Spotify AB
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

from gordon_gcp import exceptions
from gordon_gcp.schema import validate


@pytest.fixture
def json_schema_dict():
    return {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'id': 'fake schema for unit testing',
        'type': 'object',
        'properties': {
            'foo': {
                'type': 'string',
            },
            'bar': {
                'type': 'string'
            }
        },
        'required': ['foo', 'bar']
    }


@pytest.fixture
def json_schema_file(json_schema_dict, tmpdir):
    fake_schema_json_file = tmpdir.mkdir('test_validate').join('fake.json')
    fake_schema_json_file.write(json.dumps(json_schema_dict))
    return fake_schema_json_file


def test_load_schema(json_schema_file, json_schema_dict, monkeypatch, caplog):
    """Successfully load schemas."""
    monkeypatch.setattr(
        validate.MessageValidator, 'SCHEMA_DIR', json_schema_file.dirname)

    validator = validate.MessageValidator()

    assert {'fake': json_schema_dict} == validator.schemas
    assert 1 == len(caplog.records)


#####
# Non-pytest-like fixtures for parametrization
#####
def no_schemas_found(mocker, monkeypatch, tmpdir):
    mock_path = mocker.MagicMock(pathlib.Path.absolute, autospec=True)
    mock_path.return_value = []
    monkeypatch.setattr(validate.pathlib.Path, 'glob', mock_path)

    return 'Unable to load any schemas.'


def no_file_found(mocker, monkeypatch, tmpdir):
    filename = 'nope.json'
    dir_no_contents = tmpdir.mkdir('test_validate_file_not_found')
    nonexistent_file = os.path.join(dir_no_contents.dirname, filename)
    nonexistent_file = pathlib.Path(dir_no_contents.dirname, filename)
    path_glob_ret = iter([nonexistent_file])

    mock_path = mocker.MagicMock(pathlib.Path.glob, autospec=True)
    mock_path.name = filename
    mock_path.return_value = path_glob_ret
    monkeypatch.setattr(validate.pathlib.Path, 'glob', mock_path)

    return f'Error loading schema "nope"'


def json_decode_error(mocker, monkeypatch, tmpdir):
    dirname, filename = 'test_validate_json_error', 'fake.json'
    invalid_json_file = tmpdir.mkdir(dirname).join(filename)
    invalid_json_file.write('not valid json')
    invalid_json_file = pathlib.Path(invalid_json_file)

    mock_path = mocker.MagicMock(pathlib.Path.glob, autospec=True)
    mock_path.name = filename
    mock_path.return_value = [invalid_json_file]
    monkeypatch.setattr(validate.pathlib.Path, 'glob', mock_path)

    return f'Error loading schema "fake"'


args = 'fixture'
params = [
    no_schemas_found,
    no_file_found,
    json_decode_error
]


@pytest.mark.parametrize(args, params)
def test_load_schema_raises(fixture, mocker, monkeypatch, tmpdir, caplog):
    """Raise & log error if issues loading schemas."""
    exp_error_msg = fixture(mocker, monkeypatch, tmpdir)
    with pytest.raises(exceptions.GCPGordonError) as e:
        validate.MessageValidator()

    e.match(exp_error_msg)
    assert 1 == len(caplog.records)
    assert 'ERROR' == caplog.records[0].levelname
