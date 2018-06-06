Configuration
=============

Configuring Google Cloud Platform :doc:`plugins` for the `gordon`_ service.


Example Gordon Configuration with GCP Plugin
--------------------------------------------

.. literalinclude:: ../gordon.toml.example
   :language: ini


Plugin Configuration
--------------------

.. attention::

    Configuration defined for a specific provider (``event_consumer``, ``enricher``, ``publisher``) will overwrite values of the same keys defined under :ref:`gcp <gcp>`, then inherit the rest.

.. attention::

    A specific provider does **not** have access to configuration for the other individual providers.

.. note::

    Any configuration key/value listed here may also be used in the specific plugin configuration. Values set in a plugin-specific config section will overwrite what's set in this general ``[gcp]`` section.


.. _gcp:

``[gcp]``
~~~~~~~~~

.. option:: keyfile="/path/to/keyfile.json"

    `Required`: Path to the Service Account JSON keyfile to use while authenticating against Google APIs.

    While one global key for all plugins is supported, it's advised to create a key per plugin with only the permissions it requires. To setup a service account, follow `Google's docs on creating & managing service account keys <keyfiles>`_.


.. option:: project="STR"

    `Required`: Google Project ID which hosts the relevant GCP services (e.g. Cloud DNS, Pub/Sub, Compute Engine).

    To learn more about GCP projects, please see `Google's docs on creating & managing projects <projects>`_.

``[gcp.event_consumer]``
~~~~~~~~~~~~~~~~~~~~~~~~

All configuration options above in the general ``[gcp]`` may be used here. There are no specific Google Pub/Sub Consumer-related configuration options.


``[gcp.enricher]``
~~~~~~~~~~~~~~~~~~

All configuration options above in the general ``[gcp]`` may be used here. There are no specific Google Compute Engine Enricher-related configuration options.


``[gcp.publisher]``
~~~~~~~~~~~~~~~~~~~

All configuration options above in the general ``[gcp]`` may be used here. There are no specific Google DNS Publisher-related configuration options.



.. _`gordon`: https://github.com/spotify/gordon
.. _`keyfiles`: https://cloud.google.com/iam/docs/creating-managing-service-account-keys
.. _`projects`: https://cloud.google.com/resource-manager/docs/creating-managing-projects
