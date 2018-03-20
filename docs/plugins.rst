Plugins
=======

Currently available Google Cloud Platform plugins for the `gordon`_ service.

.. attention::

    These plugins are internal modules for the core `gordon`_ logic. No other use cases are expected.

.. todo::

    Add prose documentation for how to implement a plugin.

Enricher
--------

.. automodule:: gordon_gcp.plugins.enricher
.. autoclass:: gordon_gcp.GCEEnricher
    :members:


Event Consumer
--------------

.. automodule:: gordon_gcp.plugins.event_consumer
.. autoclass:: gordon_gcp.GPSEventConsumer
    :members:

Publisher
----------

.. automodule:: gordon_gcp.plugins.publisher
.. autoclass:: gordon_gcp.GDNSPublisher
    :members:


.. _`gordon`: https://github.com/spotify/gordon
