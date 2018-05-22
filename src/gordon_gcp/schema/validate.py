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
"""
Module to load and validate GCP-related JSON schemas.

Schema file discovery is based on any JSON file in the
`gordon_gcp/schema/schemas/
<https://github.com/spotify/gordon-gcp/tree/master/gordon_gcp/schemas>`_
directory.

For more information on this plugin's schemas, see :doc:`schema`.

.. warning::

    The following documentation may change since the calling/using of
    the this module is not yet incorporated into the plugin logic.

Schemas are loaded once at service startup. A JSON message/object is
validated against a given loaded schema upon receipt of message/object.
The schema to validate against is determined by the topic from which the
PubSub message is pulled.

To use:

.. code-block:: pycon

    >>> from gordon_gcp.schema import validate
    >>> validator = validate.MessageValidator()
    >>> validator.schemas
    {
        'event': {
            'title': 'Generic Event Message',
            'type': 'object',
            'required': ...},
        'audit-log': {
            'title': 'Google Audit Log Message',
            'type': 'object',
            'required': ...}
    }
    >>> example_message = {'foo': 'bar'}
    >>> validator.validate(example_message, 'event')
"""

import json
import logging
import os
import pathlib

import jsonschema

from gordon_gcp import exceptions


__all__ = ('MessageValidator',)


class MessageValidator:
    """Load packaged JSON schemas and validate a given JSON message.

    Attributes:
        schemas (dict): schema name based on filename mapped to its
            loaded JSON schema.

    Raises:
        GCPGordonError: if unable to find or load schemas.
    """

    HERE = os.path.dirname(__file__)
    SCHEMA_DIR = 'schemas'

    def __init__(self):
        self.schemas = self._load_schemas()

    def _load_schemas(self):
        schema_path = pathlib.Path(self.HERE, self.SCHEMA_DIR).absolute()
        schema_path_contents = schema_path.glob('*.json')

        schemas = {}
        for schema_file in schema_path_contents:
            schema_name = schema_file.name.split('.')[0]
            try:
                with open(schema_file, 'r') as f:
                    schemas[schema_name] = json.load(f)
                    logging.info(f'Successfully loaded schema "{schema_name}".')

            except (FileNotFoundError, json.JSONDecodeError) as e:
                msg = f'Error loading schema "{schema_name}": {e}.'
                logging.error(msg, exc_info=e)
                raise exceptions.GCPGordonError(msg)

        if not schemas:
            msg = 'Unable to load any schemas.'
            logging.error(msg)
            raise exceptions.GCPGordonError(msg)

        return schemas

    def validate(self, message, schema_name):
        """Validate a message given a schema.

        Args:
            message (dict): Loaded JSON of pulled message from Google
                PubSub.
            schema_name (str): Name of schema to validate ``message``
                against. ``schema_name`` will be used to look up
                schema from :py:attr:`.MessageValidator.schemas` dict
        Raises:
            InvalidMessageError: if message is invalid against the
                given schema.
            InvalidMessageError: if given schema name can not be found.
        """
        err = None
        try:
            jsonschema.validate(message, self.schemas[schema_name])

        except KeyError:
            msg = (f'Schema "{schema_name}" was not found (available: '
                   f'{", ".join(self.schemas.keys())})')
            err = {'msg': msg}

        except jsonschema.ValidationError as e:
            msg = (f'Given message was not valid against the schema '
                   f'"{schema_name}"')
            err = {'msg': msg, 'exc_info': e}

        if err:
            logging.error(**err)
            raise exceptions.InvalidMessageError(err['msg'])
