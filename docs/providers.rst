Providers for GCP-related Services
==================================

.. TODO: externally link "interfaces" to gordon-dns's docs once published.

This package provides the following interface providers. Use any or all of them; they are not dependent on each other.


Provider APIs
-------------

.. toctree::
   :maxdepth: 1

   compute
   dns
   pubsub


Example Gordon Configuration with GCP Plugin
--------------------------------------------

.. literalinclude:: ../gordon.toml.example
   :language: ini


Plugin Configuration
--------------------

.. attention::

    Configuration defined for a specific provider (``compute``, ``dns``, ``pubsub``) will overwrite values of the same keys defined under :ref:`gcp <gcp>`, then inherit the rest.

    A specific provider does **not** have access to configuration for the other individual providers.


.. _gcp:

``[gcp]``
~~~~~~~~~

.. program:: gcp

Global configuration for all GCP plugin providers.

.. option:: project = "STR"

    GCP `project`_ for all providers to act within.

.. option:: keyfile = "STR"

    Key needed for all providers to use when interacting with the given ``project``.


.. _gcpgce:

``[gcp.compute]``
~~~~~~~~~~~~~~~~~

.. program:: gcp.compute

Configuration specific to the :doc:`Google Compute Engine <compute>` provider.


.. option:: project = "STR"

.. option:: keyfile = "STR"

.. _gcpdns:

``[gcp.dns]``
~~~~~~~~~~~~~

.. program:: gcp.dns

Configuration specific to the :doc:`Google Cloud DNS <dns>` provider.


.. option:: project = "STR"

.. option:: keyfile = "STR"


.. _gcppubsub:

``[gcp.pubsub]``
~~~~~~~~~~~~~~~~

.. program:: gcp.pubsub

Configuration specific to the :doc:`Google Cloud PubSub <pubsub>` provider.


.. option:: project = "STR"

.. option:: keyfile = "STR"


.. _`project`: https://cloud.google.com/resource-manager/docs/cloud-platform-resource-hierarchy#projects

