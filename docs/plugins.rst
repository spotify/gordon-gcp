Plugins
=======

Currently available Google Cloud Platform plugins for the `gordon`_ and `gordon-janitor`_ services.

.. attention::

    These plugins are internal modules for the core `gordon`_ and `gordon-janitor`_ logic. No other use cases are expected.

.. todo::

    Add prose documentation for how to implement a plugin.

Gordon Service
==============

Enricher
--------

.. automodule:: gordon_gcp.plugins.service.enricher
.. autoclass:: gordon_gcp.GCEEnricher
    :members:


Event Consumer
--------------

.. automodule:: gordon_gcp.plugins.service.event_consumer
.. autoclass:: gordon_gcp.GPSEventConsumer
    :members:

GDNS Publisher
--------------

.. automodule:: gordon_gcp.plugins.service.gdns_publisher
.. autoclass:: gordon_gcp.GDNSPublisher
    :members:

Gordon Janitor
==============

Reconciler
----------

.. automodule:: gordon_gcp.plugins.janitor.reconciler
.. autoclass:: gordon_gcp.GDNSReconciler
    :members:

GPubSub Publisher
-----------------

.. automodule:: gordon_gcp.plugins.janitor.gpubsub_publisher
.. autoclass:: gordon_gcp.GPubsubPublisher
    :members:

Authority
---------

.. automodule:: gordon_gcp.plugins.janitor.authority
.. autoclass:: gordon_gcp.GCEAuthority
    :members:



.. _`gordon`: https://github.com/spotify/gordon
.. _`gordon-janitor`: https://github.com/spotify/gordon-janitor
