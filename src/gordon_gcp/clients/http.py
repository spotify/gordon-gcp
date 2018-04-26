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

To use:

.. code-block:: python

    import gordon_gcp

    keyfile = '/path/to/service_account_keyfile.json'
    auth_client = gordon_gcp.GAuthClient(keyfile=keyfile)

    client = AIOConnection(auth_client=auth_client)
    resp = await client.request('get', 'http://api.example.com/foo')

"""

import datetime
import functools
import http.client.UNAUTHORIZED
import json
import logging

import aiohttp
import backoff

from gordon_gcp import exceptions
from gordon_gcp.clients import _utils


__all__ = ('AIOConnection',)


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

    async def set_valid_token(self):
        """Check for validity of token, and refresh if none or expired."""
        is_valid = False

        if self._auth_client.creds.token:
            # Account for a token near expiration
            now = datetime.datetime.utcnow()
            skew = datetime.timedelta(seconds=60)
            if self._auth_client.creds.expiry > (now + skew):
                is_valid = True

        if not is_valid:
            await self._auth_client.refresh_token()

    async def request(self, method, url, params=None, body=None, headers=None,
                      retry_type="none", tries=1, timeout=None,
                      retry_predicate=None, predicate_args=None,
                      predicate_kwargs=None, extra_401_retry=True):
        """Wrapper for _request() that handles retries and backoffs.

        Args:
            method (str): HTTP method to use for the request.
            url (str): URL to be requested.
            params (dict): (optional) Query parameters for the request.
                Defaults to ``None``.
            body (obj): (optional) A dictionary, bytes, or file-like
                object to send in the body of the request.
            headers (dict): (optional) HTTP headers to send with the
                request. Headers pass through to the request will
                include :attr:`DEFAULT_REQUEST_HEADERS`.
            retry_type (str): (optional) The type of retries to use: "none",
                "simple", (retry up to tries/timeout), "no4xx" (don't retry
                on 4xx, otherwise up to tries/timeout), or "custom".  If
                "custom", uses the retry predicate specified in the other
                parameters.  See also extra_401_retry.
            tries (int): (optional) Total maximum number of times to try the
                request, including the first (i.e. 1 == no retries).  The number
                of actual tries is subject to retry_type and timeout as well as
                this value.
            timeout (int): (optional) Total maximum seconds to keep trying the
                request (None == unlimited).  The number of actual tries is subject to retry_type and tries as well as this value.
            retry_predicate (callable): (optional) Ignored unless retry_type is
                "custom".  Last argument must be the response from _request().
                Must return True for "continue retrying (subject to tries and
                timeout)" or False for "stop retrying".  If the function takes
                any other arguments, their values must be supplied in
                predicate_args and/or predicate_kwargs.
            predicate_args (list): (optional) See retry_predicate.
            predicate_kwargs (dict): (optional) See retry_predicate.
            extra_401_retry (bool): (optional) If True, run one more request
                attempt on a 401 (unauthorized), regardless of the value of
                retry_type.  This makes sure we've tried generating a new token.
        Returns:
            (str) HTTP response body.
        Raises:
            :exc:`.GCPHTTPError`: If a response code >=400 was received and
                there are no more tries (or no retrying is specified).
        """
        retry_predicates = {
            "none": lambda resp: False,  # never retry
            "simple": lambda resp: True,  # always retry up to tries/timeout
            # don't retry on 4xx
            "no4xx": lambda resp: resp.status < 400 or resp.status >=500,
            "custom": retry_predicate,
        }
        actual_predicate = retry_predicates[retry_type]
        if predicate_args is None:
            predicate_args = []
        if predicate_kwargs is None:
            predicate_kwargs = {}
        predicate_partial = partial(
            actual_predicate, *predicate_args, **predicate_kwargs
        )

        resp = await backoff.on_predicate(
            backoff.expo, max_tries=tries, max_time=timeout, predicate=predicate_partial, on_giveup=retry_giveup,
        )(self._request)(method, url, params, body, headers)
        # one extra try on a 401 to make sure we've refreshed the token
        if resp.status == HTTPStatus.UNAUTHORIZED and extra_401_retry:
            log_msg = ('Unauthorized. Attempting to refresh token and '
                       'try again.')
            logging.info(log_msg)
            resp = self._request(method, url, params, body, headers)

        # avoid leaky abstractions and wrap http errors with our own
        try:
            resp.raise_for_status()
        except aiohttp.ClientResponseError as e:
            msg = f'Issue connecting to {resp.url.host}: {e}'
            logging.error(msg, exc_info=e)
            raise exceptions.GCPHTTPError(msg)
        return await resp.text()

        # TODO:
        # import
        # logging.getLogger('backoff').addHandler(logging.StreamHandler())
        # logging.getLogger('backoff').setLevel(logging.INFO)

    async def _request(self, method, url, params=None, body=None, headers=None):
        """Make an asynchronous HTTP request.
        
        Args:
            See request().
        Returns:
            (aiohttp.ClientResponse) response from the request.
        """
        req_headers = headers or {}
        req_headers.update(_utils.DEFAULT_REQUEST_HEADERS)

        await self.set_valid_token()
        req_headers.update(
            {'Authorization': f'Bearer {self._auth_client.token}'}
        )

        req_kwargs = {
            'params': params,
            'data': body,
            'headers': req_headers,
        }
        logging.debug(_utils.REQ_LOG_FMT.format(method=method.upper(), url=url))
        async with self._session.request(method, url, **req_kwargs) as resp:
            log_kw = {
                'method': method.upper(),
                'url': url,
                'status': resp.status,
                'reason': resp.reason
            }
            logging.debug(_utils.RESP_LOG_FMT.format(**log_kw))
            return resp

    async def get_json(self, url, json_callback=None, retry_type="none",
                       tries=1, timeout=None, retry_predicate=None,
                       predicate_args=None, predicate_kwargs=None,
                       extra_401_retry=True):
        """Get a URL and return its JSON response.

        Args:
            url (str): URL to be requested.
            json_callback (func): Custom JSON loader function. Defaults
                to :meth:`json.loads`.
            retry_type (str): (optional) See request().
            tries (int): (optional) See request().
            timeout (int): (optional) See request().
            retry_predicate (callable): (optional) See request().
            predicate_args (list): (optional) See request().
            predicate_kwargs (dict): (optional) See request().
            extra_401_retry (bool): (optional) See request().
        Returns:
            response body returned by :func:`json_callback` function.
        """
        if not json_callback:
            json_callback = json.loads
        response = await self.request(
            method='get', url=url, retry_type=retry_type, tries=tries,
            timeout=timeout, retry_predicate=retry_predicate,
            predicate_args=predicate_args, predicate_kwargs=predicate_kwargs,
            extra_401_retry=extra_401_retry,
        )
        return json_callback(response)
