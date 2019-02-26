Changelog
=========

0.0.1.dev32 (2019-02-26)
------------------------

Changed
~~~~~~~

* Exit (code=1) on failure to create pubsub subscription.

Fixed
~~~~~

* Fix reconciler bug introduced in v0.0.1.dev31.

0.0.1.dev31 (2019-02-18)
------------------------

Updated
~~~~~~~

* Adjust metrics for graph optimization and to include source in janitor.


0.0.1.dev30 (2018-12-12)
------------------------

Added
~~~~~

* Add metrics for record dispatching.

0.0.1.dev29 (2018-12-05)
------------------------

Added
~~~~~
* Add project_whitelist config to janitor authority.

0.0.1.dev28 (2018-10-19)
------------------------

Added
~~~~~
* Allow supplying custom managed zone name prefix.


0.0.1.dev27 (2018-10-16)
------------------------

Removed
~~~~~~~
* Revert filtering out instances with status = TERMINATED in the authority.


0.0.1.dev26 (2018-10-15)
------------------------

Added
~~~~~
* Filter out instances with status = TERMINATED in the authority.

Changed
~~~~~~~
* Lower minimum TTL for the event schema to 60.

Fixed
~~~~~
* Correct an external URL used in doc generation.


0.0.1.dev25 (2018-09-20)
------------------------

Removed
~~~~~~~
* Removed superfluous jsonschema validation logging.


0.0.1.dev23 & 0.0.1.dev24 (2018-09-12/13)
-----------------------------------------

Added
~~~~~
* Add logging on aiohttp request calls.


0.0.1.dev22 (2018-09-10)
------------------------

Fixed
~~~~~
* Fix incorrect function call in GDNS publisher.


0.0.1.dev21 (2018-09-07)
------------------------

Changed
~~~~~~~
* Bump upstream Gordon package requirements.


0.0.1.dev20 (2018-09-07)
------------------------

Fixed
~~~~~
* Fix bug in date comparison in drop-old-message functionality.

Added
~~~~~
* Add metrics to the janitor.

Changed
~~~~~~~
* Speed up the reconciler.


0.0.1.dev19 (2018-08-14)
------------------------

Fixed
~~~~~
* Fix paging of GDNS responses.
* Make setup.py work with direct github commit links.

Added
~~~~~
* Drop messages older than a limit.

Changed
~~~~~~~
* Remove managed zone from plugin configuration in favor of automated conversion from DNS zone.
* Do DNS zone to managed zone conversion only in GDNSClient.


0.0.1.dev18 (2018-08-01)
------------------------

Added
~~~~~
* Add HTTP 403 response code to janitor PROJECT_SKIP_RESP_CODES constant.

Changed
~~~~~~~
* Default max results to 100 when listing instances in GCE client.


0.0.1.dev17 (2018-07-27)
------------------------

Added
~~~~~
* Add callback in pubsub publisher.


0.0.1.dev16 (2018-07-26)
------------------------

Fixed
~~~~~
* Fix incorrect and superfluous authority logging.

Added
~~~~~
* Add deletions to the janitor reconciler.

Changed
~~~~~~~
* Simplify HTTP error response handling.
* Fail authority if it cannot get a full view of of all instances.

Removed
~~~~~~~
* Removed GCPHTTPNotFoundError and GCPHTTPConflictError.


0.0.1.dev15 (2018-07-17)
------------------------

Changed
~~~~~~~
* Increase level of detail in HTTP request/response logging.

Fixed
~~~~~
* Properly support 'deletions' action.


0.0.1.dev14 (2018-07-10)
------------------------

Added
~~~~~

* Add ``kind`` attribute to ``GCPResourceRecordSet`` object.
* Add request concurrency to GCE listing of instances.


0.0.1.dev13 (2018-07-03)
------------------------

Changed
~~~~~~~

* Update gordon-cloud-pubsub version to ``0.35.4``.

Removed
~~~~~~~

* Remove the use of ``_GPSThreads``.


0.0.1.dev12 (2018-06-28)
------------------------

Fixed
~~~~~
* Clean up GPThread instances once done.


0.0.1.dev11 (2018-06-25)
------------------------

Changed
~~~~~~~
* Janitor: Skip project if listing instances fails.
* Extract response rrsets properly.
* Make params optional when calling http.get_all.


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
