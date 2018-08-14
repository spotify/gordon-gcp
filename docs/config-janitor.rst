Janitor Configuration
=====================

Configuring Google Cloud Platform :doc:`plugins` for the `gordon-janitor`_ service.

Example Configuration
---------------------

An example of a ``gordon-janitor.toml`` file for GCP-specific plugins:

.. literalinclude:: ../gordon-janitor.toml.example
    :language: ini


Plugin Configuration
--------------------

The following sections are supported:


gcp
~~~

Any configuration key/value listed here may also be used in the specific plugin configuration. Values set in a plugin-specific config section will overwrite what's set in this general ``[gcp]`` section.

.. option:: keyfile="/path/to/keyfile.json"

    `Required`: Path to the Service Account JSON keyfile to use while authenticating against Google APIs.

    While one global key for all plugins is supported, it's advised to create a key per plugin with only the permissions it requires. To setup a service account, follow `Google's docs on creating & managing service account keys <https://cloud.google.com/iam/docs/creating-managing-service-account-keys>`_.

    .. attention::

        For the Pub/Sub plugin, ``keyfile`` is not required when running against the `Pub/Sub Emulator <https://cloud.google.com/pubsub/docs/emulator>`_ that Google provides.

.. option:: project="STR"

    `Required`: Google Project ID which hosts the relevant GCP services (e.g. Cloud DNS, Pub/Sub, Compute Engine).

    To learn more about GCP projects, please see `Google's docs on creating & managing projects <https://cloud.google.com/resource-manager/docs/creating-managing-projects>`_.

.. option:: scopes=["STR","STR"]

    `Optional`: A list of strings of the scope(s) needed when making calls to Google APIs. Defaults to ``["cloud-platform"]``.

.. option:: cleanup_timeout=INT

    `Optional`: Timeout in seconds for how long each plugin should wait for outstanding tasks (e.g. processing remaining message from a channel) before cancelling. This is only used when a plugin has received all messages from a channel, but may have work outstanding. Defaults to ``60``.


gcp.gdns
~~~~~~~~

All configuration options above in the general ``[gcp]`` may be used here. There are no specific DNS-related configuration options.

gcp.gpubsub
~~~~~~~~~~~

All configuration options above in the general ``[gcp]`` may be used here. Additional Google Cloud Pub/Sub-related configuration is needed:

.. option:: topic="STR"

    `Required`: Google Pub/Sub topic to receive the publish change messages.

.. attention::

    For the Pub/Sub plugin, ``keyfile`` is not required when running against the `Pub/Sub Emulator <https://cloud.google.com/pubsub/docs/emulator>`_ that Google provides.


gcp.gce
~~~~~~~

All configuration options from the general ``[gcp]`` section may be used here.

Additional plugin-specific configuration is needed:

.. option:: dns_zone="STR"

    `Required`: DNS zone to pull records from.  Must be a fully-qualified domain name (FQDN), ending in ``.``, e.g. ``example.com.``.  If it's a reverse zone, it must be in the form 'A.B.in-addr.arpa.'.

    Note: this is separate from Google's 'managed zone' names.  Google uses custom string names with specific `requirements <https://cloud.google.com/dns/api/v1/managedZones#resource>`_ for storing records. Gordon requires that managed zone names be based on DNS names. For all domains, remove the trailing dot and replace all other dots with dashes.  For reverse records, then use only the two most significant octets, prepended with 'reverse-'.  (E.g. ``foo.bar.com.`` -> ``foo-bar-com`` and ``0.168.192.in-addr.arpa.`` -> ``reverse-168-192.``)

.. option:: metadata_blackklist=[["STR","STR"],["STR","STR"]]

    `Optional`: List of key-value pairs that will be used to filter out unwanted GCE instances by `instance metadata <https://cloud.google.com/compute/docs/storing-retrieving-metadata>`_. Note that both the key and the value must match for an instance to be filtered out.

.. option:: tag_blacklist=["STR","STR"]

    `Optional`: List of `network tags <https://cloud.google.com/vpc/docs/add-remove-network-tags>`_  that will be used to filter out unwanted GCE instances.

.. option:: project_blacklist=["STR","STR"]

    `Optional`: List of unique, user-assigned project IDs (``projectId``) that will be ignored when fetching projects.

.. option:: instance_filter="STR"

    `Optional`: String used to filter instances by instance attributes. It is passed directly to GCE's `v1.instances.aggregatedList <https://cloud.google.com/compute/docs/reference/rest/v1/instances/aggregatedList>`_ endpoint's `filter` parameter.

.. _`gordon-janitor`: https://github.com/spotify/gordon-janitor
