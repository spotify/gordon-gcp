Changelog
=========

0.0.1.dev10 (2018-06-20)
------------------------

Changed
~~~~~~~
* Updated the Google API compute v1 endpoint URL.


0.0.1.dev9 (2018-06-20)
-----------------------

Added
~~~~~
* Add threadsafety when adding a message to the success channel from ``GPSEventConsumer``.
* Add flow control when consuming from Pub/Sub.

Changed
~~~~~~~
* Update interface implementation of ``GEventMessage``.


Removed
~~~~~~~
* Remove date validation in schemas.


0.0.1.dev8 (2018-06-18)
-----------------------

Changed
~~~~~~~
* Reorder args for GCEEnricher.


0.0.1.dev7 (2018-06-15)
-----------------------

Changed
~~~~~~~
* Update gordon-dns to 0.0.1.dev3.


Removed
~~~~~~~
* Remove routing logic from plugins.


0.0.1.dev6 (2018-06-07)
-----------------------

Changed
~~~~~~~

* Internal API improvements.


0.0.1.dev5 (2018-06-07)
-----------------------

Changed
~~~~~~~

* Fix failure for core to instantiate GDNSPublisher plugin.
* Internal API improvements.


0.0.1.dev4 (2018-06-05)
-----------------------

Added
~~~~~

* Merged gordon-janitor-gcp repo into gordon-gcp.
* Added janitor plugin summaries.
* Added missing exception docs.

Changed
~~~~~~~

* Updated and fixed OWNERS.
* Cleaned up some capitalizations and wordings.
* Suppressed a test warning.
* Fixed namespace collapses (``__all__`` / ``import *``).


-----------------------

Added
~~~~~

* Add implementation of IEventConsumer.
* Add implementation of IPublisher.
* Add implementation of IEnricher.
* Add support on loading credentials with application default credentials.
* Add support for ``POST`` JSON requests to HTTP client.


0.0.1.dev2 (2018-03-29)
-----------------------

Changed
~~~~~~~

Fixed packaging.


0.0.1.dev1 (2018-03-28)
-----------------------

Changed
~~~~~~~

Initial development release.
