Schemas for Event Consuming
===========================

Definitions
-----------

This plugin defines two types of schemas for consuming events by the :doc:`Google Pub/Sub Event Consumer plugin <plugins>`: :ref:`audit` and a :ref:`general`.



.. _audit:

Google Audit Log Message
~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`audit` schema is based off of Google's `Audit Log`_ datatype for `Cloud Audit Logging`_. Documentation on setting up the exporting of Google Cloud audit logs to Google Pub/Sub (a.k.a. a "sink") can be found `here`_. You may need to refine the export to include the following `filters`_::


    resource.type=gce_instance
    protoPayload."@type"="type.googleapis.com/google.cloud.audit.AuditLog"


.. _schemadef:

Schema Definition
^^^^^^^^^^^^^^^^^

.. literalinclude:: ../src/gordon_gcp/schema/schemas/audit-log.schema.json
    :language: json

Examples
^^^^^^^^

Audit Log Message with an ``operation.first`` key:
**************************************************

.. literalinclude:: ../src/gordon_gcp/schema/examples/audit-log.first-operation.json
    :language: json

Audit Log Message with an ``operation.last`` key:
*************************************************

.. literalinclude:: ../src/gordon_gcp/schema/examples/audit-log.last-operation.json
    :language: json

Audit Log Message with no ``operation`` object:
***********************************************

.. literalinclude:: ../src/gordon_gcp/schema/examples/audit-log.no-operation.json
    :language: json


.. _general:

General Event Message
~~~~~~~~~~~~~~~~~~~~~

The :ref:`general` schema is meant for any event that may not come from Google's audit log sink. For instance, one may need to add/update/delete a manual record (i.e. marketing-friendly ``CNAME`` s). Or there may be a reconciliation process between the current state of the world and what is reflected in DNS.

Schema Definition
^^^^^^^^^^^^^^^^^

.. literalinclude:: ../src/gordon_gcp/schema/schemas/event.schema.json
    :language: json


Examples
^^^^^^^^

Event for an ``A`` record:
**************************

.. literalinclude:: ../src/gordon_gcp/schema/examples/event.A.json
    :language: json


Event for an ``CNAME`` record:
******************************

.. literalinclude:: ../src/gordon_gcp/schema/examples/event.CNAME.json
    :language: json


Event for an ``NS`` record:
***************************

.. literalinclude:: ../src/gordon_gcp/schema/examples/event.NS.json
    :language: json


Validating
----------


.. automodule:: gordon_gcp.schema.validate
.. autoclass:: gordon_gcp.MessageValidator
    :members:


Parsing
-------

.. automodule:: gordon_gcp.schema.parse
.. autoclass:: gordon_gcp.MessageParser
    :members:

.. _`Audit Log`: https://cloud.google.com/logging/docs/reference/audit/auditlog/rest/Shared.Types/AuditLog
.. _`Cloud Audit Logging`: https://cloud.google.com/logging/docs/audit/
.. _`here`: https://cloud.google.com/logging/docs/audit/#exporting_audit_logs
.. _`filters`: https://cloud.google.com/logging/docs/view/advanced-filters
