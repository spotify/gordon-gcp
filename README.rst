=========================================================
``gordon-gcp``: GCP Plugins for Gordon and Gordon Janitor
=========================================================

*Event-driven Cloud DNS and DNS Reconciliation Services*

.. desc-begin

Google Cloud Platform (GCP) plugins for `gordon`_, an open-source, event-driven service for 3rd party DNS providers, and for `gordon-janitor`_, an open-source service that checks Cloud DNS records against a source of truth (e.g. Compute Engine) and submits corrections to `gordon`_ via Google Pubsub.

The ``gordon-gcp`` plugins add optional support for the following:

Service:

* Consuming events from `Google Cloud Pub/Sub`_
* Reading from `Google Compute Engine`_ for record information
* Creating, updating, and deleting records within `Google Cloud DNS`_

Janitor:

* Reading instance lists from `Google Compute Engine`_
* Comparing record sets from `Google Compute Engine`_ and `Google Cloud DNS`_
* Publishing any required DNS changes to `Google Cloud Pub/Sub`_

.. desc-end

**NOTICE**: This is still in the planning phase and under active development. Gordon, Gordon Janitor, and these plugins should not be used in production yet.

.. intro-begin

Requirements
============

* Python 3.6
* Google Cloud Platform account
* Service account JSON key that has relevant access (i.e. read and/or write) to the plugin service you want to use (e.g. Google Cloud DNS, Pub/Sub, or Compute Engine). See Google's `documentation`_ on how to create a key.

Support for other Python versions may be added in the future.

Development
===========

For development and running tests, your system must have all supported versions of Python installed. We suggest using `pyenv`_.

Setup
-----

.. code-block:: bash

    $ git clone git@github.com:spotify/gordon-gcp.git && cd gordon-gcp
    # make a virtualenv
    (env) $ pip install -r dev-requirements.txt

Running tests
-------------

To run the entire test suite:

.. code-block:: bash

    # outside of the virtualenv
    # if tox is not yet installed
    $ pip install tox
    $ tox

If you want to run the test suite for a specific version of Python:

.. code-block:: bash

    # outside of the virtualenv
    $ tox -e py36

To run an individual test, call ``pytest`` directly:

.. code-block:: bash

    # inside virtualenv
    (env) $ pytest tests/test_foo.py


Build docs
----------

To generate documentation:


.. code-block:: bash

    (env) $ pip install -r docs-requirements.txt
    (env) $ cd docs && make html  # builds HTML files into _build/html/
    (env) $ cd _build/html
    (env) $ python -m http.server $PORT


Then navigate to ``localhost:$PORT``!

To watch for changes and automatically reload in the browser:

.. code-block:: bash

    (env) $ cd docs
    (env) $ make livehtml  # default port 8888
    # to change port
    (env) $ make livehtml PORT=8080


Code of Conduct
===============

This project adheres to the `Open Code of Conduct`_. By participating, you are expected to honor this code.

.. _`pyenv`: https://github.com/yyuu/pyenv
.. _`Open Code of Conduct`: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
.. _`Google Cloud DNS`: https://cloud.google.com/dns/docs
.. _`Google Cloud Pub/Sub`: https://cloud.google.com/pubsub/docs
.. _`Google Compute Engine`: https://cloud.google.com/compute/docs
.. _`gordon`: https://github.com/spotify/gordon
.. _`gordon-janitor`: https://github.com/spotify/gordon-janitor
.. _`documentation`: https://cloud.google.com/iam/docs/creating-managing-service-account-keys
