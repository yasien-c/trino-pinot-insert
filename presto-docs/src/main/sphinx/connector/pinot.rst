===============
Pinot Connector
===============

The Pinot connector allows Presto to query data stored in
`Apache Pinot™ (Incubating) <https://pinot.apache.org/>`_.

Compatibility
-------------

The Pinot connector is compatible with all Pinot versions starting from 0.1.0.

Configuration
-------------

To configure the Pinot connector, create a catalog properties file
``etc/catalog/pinot.properties`` with atleast the following contents,
replacing ``host1:9000,host2:9000`` with a comma-separated list of Pinot Controller nodes

.. code-block:: none

    connector.name=pinot
    pinot.controller-urls=host1:9000,host2:9000

Configuration Properties
------------------------

The following configuration properties are available:

================================================== ========== ===================================================================================
Property Name                                      Required   Description
================================================== ========== ===================================================================================
``pinot.controller-urls``                          Yes        A comma separated list of controller hosts. If Pinot is deployed via
                                                              `Kubernetes <https://kubernetes.io/>`_ this needs to point to the controller
                                                              service endpoint. The Pinot broker and server must be accessible via dns as
                                                              Pinot will return hostnames and not ip addresses.
``pinot.segments-per-split``                       No         The number of segments processed in a split. Setting this higher will reduce
                                                              the number of requests made to Pinot which makes sense for a smaller Pinot cluster.
``pinot.request-timeout``                          No         The timeout for Pinot requests. Increasing this can reduce timeouts if dns
                                                              resolution is slow.
================================================== ========== ===================================================================================

Querying Pinot Tables
-------------------------

The schema for each Pinot catalog is ``default``. To find all tables you can run::

    SHOW TABLES FROM pinot.default;

To see a list of columns in the ``flight_status`` table the following can be run:

    DESCRIBE pinot.default.flight_status;
    SHOW COLUMNS FROM pinot.default.flight_status;

Tables can be queried as they are in other connectors, leveraging filter and limit pushdown:

    SELECT foo FROM pinot_table WHERE bar = 3 AND baz IN ('ONE', 'TWO', 'THREE') LIMIT 25000;

Dynamic Tables
--------------

To leverage Pinot's fast aggregation, a Pinot query written in PQL can be used as the table name.
Filters and limits in the outer query will be pushed down to Pinot.
Some examples:

    SELECT *
    FROM pinot.default."SELECT MAX(col1), COUNT(col2) FROM pinot_table GROUP BY col3, col4"
    WHERE col3 IN ('FOO', 'BAR') AND col4 > 50
    LIMIT 30000

These queries are routed to the broker and are more suitable to aggregate queries.
For regular ``SELECT`` queries it is more performant to issue a regular presto query which will be routed to the servers
which store the data.

The above query would yield the following Pinot PQL query:

    SELECT MAX(col1), COUNT(col2)
    FROM pinot_table
    WHERE col3 IN('FOO', 'BAR') and col4 > 50
    TOP 30000


If you are returning a larger dataset you can issue a normal Presto query which will get routed to the Pinot servers which
store the Pinot segments. Filters and Limits are pushed down to Pinot for regular queries as well.

Data types
----------

Pinot supports the following primitive types which currently not allow null values:

==========================   ============
Pinot                        Presto
==========================   ============
``INT``                      ``INTEGER``
``LONG``                     ``BIGINT``
``FLOAT``                    ``REAL``
``DOUBLE``                   ``DOUBLE``
``STRING``                   ``VARCHAR``
``INT_ARRAY``                ``VARCHAR``
``LONG_ARRAY``               ``VARCHAR``
``FLOAT_ARRAY``              ``VARCHAR``
``DOUBLE_ARRAY``             ``VARCHAR``
``STRING_ARRAY``             ``VARCHAR``
==========================   ============

