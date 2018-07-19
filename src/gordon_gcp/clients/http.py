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
Module to interact with Google APIs via asynchronous HTTP calls.
:class:`.AIOConnection` is meant to be used/inherited by other
product-specific API clients as it handles Google authentication and
automatic refresh of tokens.

.. todo::

    Include that it also handles retries once implemented.

To use:

.. code-block:: python

    import gordon_gcp

    keyfile = '/path/to/service_account_keyfile.json'
    auth_client = gordon_gcp.GAuthClient(keyfile=keyfile)

    client = AIOConnection(auth_client=auth_client)
    resp = await client.request('get', 'http://api.example.com/foo')

"""

import datetime
import http.client
import json
import logging
import uuid

import aiohttp

from gordon_gcp import exceptions
from gordon_gcp.clients import _utils


__all__ = ('AIOConnection',)

REFRESH_STATUS_CODES = (http.client.UNAUTHORIZED,)


class AIOConnection:
    """Async HTTP client to Google APIs with service-account-based auth.

    Args:
        auth_client (.GAuthClient): client to manage authentication for
            HTTP API requests.
        session (aiohttp.ClientSession): (optional) ``aiohttp`` HTTP
            session to use for sending requests. Defaults to the session
            object attached to :obj:`auth_client` if not provided.
    """

    def __init__(self, auth_client=None, session=None):
        self._auth_client = auth_client
        self._session = session or auth_client._session

    async def valid_token_set(self):
        """Check for validity of token, and refresh if none or expired."""
        is_valid = False

        if self._auth_client.token:
            # Account for a token near expiration
            now = datetime.datetime.utcnow()
            skew = datetime.timedelta(seconds=60)
            if self._auth_client.expiry > (now + skew):
                is_valid = True
        return is_valid

    async def request(self, method, url, params=None, headers=None,
                      data=None, json=None, token_refresh_attempts=2,
                      **kwargs):
        """Make an asynchronous HTTP request.

        Args:
            method (str): HTTP method to use for the request.
            url (str): URL to be requested.
            params (dict): (optional) Query parameters for the request.
                Defaults to ``None``.
            headers (dict): (optional) HTTP headers to send with the
                request. Headers pass through to the request will
                include :attr:`DEFAULT_REQUEST_HEADERS`.
            data (obj): (optional) A dictionary, bytes, or file-like
                object to send in the body of the request.
            json (obj): (optional) Any json compatible python
                object.
                NOTE: json and body parameters cannot be used at the same time.
            token_refresh_attempts (int): (optional) Number of attempts a token
                refresh should be performed.
        Returns:
            (str) HTTP response body.
        Raises:
            :exc:`.GCPHTTPError`: if any exception occurred,
                specifically a :exc:`.GCPHTTPResponseError`, if the
                exception is associated with a response status code.

        """
        if all([data, json]):
            msg = ('"data" and "json" request parameters can not be used '
                   'at the same time')
            logging.warn(msg)
            raise exceptions.GCPHTTPError(msg)

        req_headers = headers or {}
        req_headers.update(_utils.DEFAULT_REQUEST_HEADERS)
        req_kwargs = {
                'params': params,
                'headers': req_headers,
            }

        if data:
            req_kwargs['data'] = data
        if json:
            req_kwargs['json'] = json

        if token_refresh_attempts:
            if not await self.valid_token_set():
                await self._auth_client.refresh_token()
                token_refresh_attempts -= 1

        req_headers.update(
            {'Authorization': f'Bearer {self._auth_client.token}'}
        )

        request_id = kwargs.get('request_id', uuid.uuid4())
        logging.debug(_utils.REQ_LOG_FMT.format(
            request_id=request_id,
            method=method.upper(),
            url=url,
            kwargs=req_kwargs))
        async with self._session.request(
                method, url, **req_kwargs) as resp:
            log_kw = {
                'request_id': request_id,
                'method': method.upper(),
                'url': resp.url,
                'status': resp.status,
                'reason': resp.reason
            }
            logging.debug(_utils.RESP_LOG_FMT.format(**log_kw))

            if resp.status in REFRESH_STATUS_CODES:
                logging.warning(f'[{request_id}] HTTP Status Code {resp.status}'
                                f' returned requesting {resp.url}: '
                                f'{resp.reason}')
                if token_refresh_attempts:
                    logging.info(f'[{request_id}] Attempting request to '
                                 f'{resp.url} again.')
                    return await self.request(
                        method, url,
                        token_refresh_attempts=token_refresh_attempts,
                        request_id=request_id,
                        **req_kwargs)

                logging.warning(f'[{request_id}] Max attempts refreshing auth '
                                f'token exhausted while requesting {resp.url}')

            # avoid leaky abstractions and wrap http errors with our own
            try:
                resp.raise_for_status()
            except aiohttp.ClientResponseError as e:
                msg = f'[{request_id}] Issue connecting to {resp.url}: {e}'
                logging.error(msg, exc_info=e)
                raise exceptions.GCPHTTPResponseError(msg, resp.status)

            return await resp.text()

    async def get_json(self, url, json_callback=None, **kwargs):
        """Get a URL and return its JSON response.

        Args:
            url (str): URL to be requested.
            json_callback (func): Custom JSON loader function. Defaults
                to :meth:`json.loads`.
            kwargs (dict): Additional arguments to pass through to the
                request.
        Returns:
            response body returned by :func:`json_callback` function.
        """
        if not json_callback:
            json_callback = json.loads
        response = await self.request(method='get', url=url, **kwargs)
        return json_callback(response)

    async def get_all(self, url, params=None):
        """Aggregate data from all pages of an API query.

        Args:
            url (str): Google API endpoint URL.
            params (dict): (optional) URL query parameters.

        Returns:
            list: Parsed JSON query response results.
        """
        if not params:
            params = {}
        items = []
        next_page_token = None

        while True:
            if next_page_token:
                params['pageToken'] = next_page_token
            response = await self.get_json(url, params=params)

            items.append(response)
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        return items
