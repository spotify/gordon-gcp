Service Configuration
=====================

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

All configuration options above in the general ``[gcp]`` may be used here. Additional Google Pub/Sub Consumer-related configuration options are:

.. option:: topic="STR"

    `Required`: A topic to which the Event Consumer client must subscribe.

    For more information on Google Pub/Sub topics, please see `Google's docs on managing topics <topics>`_.

.. option:: subscription="STR"

    `Required`: A subscription to the ``topic`` from which the Event Consumer client will pull.

    For more information on Google Pub/Sub subscriptions, please see `Google's docs on managing subscriptions <subscriptions>`_.

.. option:: max_messages=INT

    `Optional`: Number of Pub/Sub messages to process at a time. Defaults to 25.


``[gcp.enricher]``
~~~~~~~~~~~~~~~~~~

All configuration options above in the general ``[gcp]`` may be used here. Additional Google Compute Engine configuration options are:


.. option:: dns_zone="STR"

    `Required`: DNS zone to validate the correctness of A records before publishing. Must be a fully-qualified domain name (FQDN), ending in ``.``, e.g. ``example.com.``.


``[gcp.publisher]``
~~~~~~~~~~~~~~~~~~~

All configuration options above in the general ``[gcp]`` may be used here. Additional Google Cloud DNS configuration options are:

.. describe:: dns_zone="STR"

    `Required`: DNS zone to validate the correctness of A records before publishing. Must be a fully-qualified domain name (FQDN), ending in ``.``, e.g. ``example.com.``.

.. option:: managed_zone="STR"

    `Required`: The managed zone name in Google Cloud DNS where records are to be published.

    To learn more about managed zones, please see `Google's docs on managed zones <managed_zones>`_.

.. option:: default_ttl=INT

    `Required`: The default TTL in seconds. This will be used if the publisher receives a record set to be published that does not yet have the TTL set. Must be greater than 4.

.. option:: publish_wait_timeout=INT|FLOAT

    `Optional`: Timeout in seconds for waiting for confirmation that changes have been successfully completed within Google Cloud DNS. Default is 60 seconds.

.. option:: api_version="STR"

    `Optional`: API version for both the `changes`_ endpoint and the `resource records`_ endpoint.


.. _`gordon`: https://github.com/spotify/gordon
.. _`keyfiles`: https://cloud.google.com/iam/docs/creating-managing-service-account-keys
.. _`projects`: https://cloud.google.com/resource-manager/docs/creating-managing-projects
.. _`topics`: https://cloud.google.com/pubsub/docs/admin#managing_topics
.. _`subscriptions`: https://cloud.google.com/pubsub/docs/admin#managing_subscriptions
.. _`managed_zones`: https://cloud.google.com/dns/zones/
.. _`changes`: https://cloud.google.com/dns/api/v1/changes
.. _`resource records`: https://cloud.google.com/dns/api/v1/resourceRecordSets/list
