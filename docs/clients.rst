API Clients
===========

.. currentmodule:: gordon_gcp

HTTP Sessions
-------------


.. TODO::
    Update prose of "...you can pass in your own session into an API client" with specific API client class names once they're defined. Also update the example code using one API client.

By default, the HTTP session used for getting credentials is reused for API calls (recommended if there are many). If this is not desired, you can pass in your own :class:`aiohttp.ClientSession` instance into an API client or :class:`AIOConnection`. The auth client :class:`GAuthClient` may also take an explicit session object, but is not required to assert a different HTTP session is used for the API calls.

.. code-block:: python

    import aiohttp
    import gordon_gcp

    keyfile = '/path/to/service_account_keyfile.json'
    session = aiohttp.ClientSession()  # optional
    auth_client = gordon_gcp.GAuthClient(
        keyfile=keyfile, session=session
    )

    new_session = aiohttp.ClientSession()

    # basic HTTP client
    client = gordon_gcp.AIOConnection(
        auth_client=auth_client, session=new_session
    )


.. NOTE: we separate out `automodule` and `autoclass` (rather than list members with automodule) to make use of the namespace flattening.


Asynchronous GCP HTTP Client
----------------------------

.. automodule:: gordon_gcp.clients.http
.. autoclass:: gordon_gcp.AIOConnection
    :members:


GCP Auth Client
---------------

.. automodule:: gordon_gcp.clients.auth
.. autoclass:: gordon_gcp.GAuthClient
    :members:

.. _app_default_creds:

.. autofunction:: google.auth._default.default