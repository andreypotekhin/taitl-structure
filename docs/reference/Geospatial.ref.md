# Geospatial Types and Helpers

Use a namespace to choose which spatial Python API an expression calls. Use the field declaration to choose the
physical type stored in a schema. These choices are independent: Apache Spark and Databricks helpers can share native
Geometry/Geography types, while Sedona uses its own Geometry UDT.

## Native Spark types

Declare native fields with `geometry` or `geography`:

    from structure.plugin.pyspark import geometry, geography

    class Places(Schema):
        shape = geometry(srid=4326, nullable=False)
        boundary = geography(srid=4326)

An integer SRID declares one fixed coordinate reference system; `None` means mixed or unknown SRIDs. Booleans and
non-integers are invalid. The Spark runtime checks whether it supports the integer SRID. Native type constructors are
available only on exact Spark 4.0 or 4.1 profiles where the selected runtime actually exposes them. On an unavailable
constructor, schema materialization raises an error linking to this reference.

The root helper namespace calls Apache PySpark:

    from structure.plugin.pyspark import st_geomfromwkb, st_geogfromwkb, st_asbinary, st_srid, st_setsrid

    shape = st_setsrid(st_geomfromwkb(row.payload), 4326)
    payload = st_asbinary(shape)

These helpers are gated to exact `>=4.1,<4.2` targets. Apache Geometry WKB construction has SRID 0; Geography has
SRID 4326. A literal SRID in `st_setsrid` produces a fixed-SRID expression; an expression-valued SRID produces mixed
SRID. No spatial arithmetic or implicit spatial cast is provided.

Spark 4.1 guards native geospatial execution. Enable the Spark session setting before using native types or helpers:

```python
spark = SparkSession.builder.config("spark.sql.geospatial.enabled", "true").getOrCreate()
```

Structure leaves this setting under caller control and surfaces Spark's native error if the feature is disabled.
The ordinary and Connect 4.1 parity runs both passed with this setting enabled.

## Databricks helpers

Call Databricks' Python API explicitly:

    from structure.plugin.pyspark import databricks

    shape = databricks.st_geomfromwkb(row.payload, 4326)

The namespace resolves to `pyspark.databricks.sql.functions`; it never falls back to Apache or Sedona. The Databricks
WKB Geometry helper accepts its optional SRID argument. Availability is gated independently on exact 4.0/4.1 targets
and checked against the active Databricks installation. Apache Spark tests and mocked imports do not prove managed
Databricks parity. Databricks Connect also requires its own live evidence.

## Sedona Geometry

Declare Sedona values using its namespace:

    from structure.plugin.pyspark import sedona

    class SedonaPlaces(Schema):
        shape = sedona.geometry(srid=4326)

    shape = sedona.st_geomfromwkt(row.wkt, 4326)
    text = sedona.st_astext(shape)

Sedona uses a distinct UDT. Initialize Sedona for the supplied Spark session with its public context API before using
the Sedona helpers:

```python
from sedona.spark import SedonaContext

spark = SedonaContext.create(spark)
```

Its supported helper set includes `st_geomfromwkt`, `st_geomfromwkb`, `st_geomfromewkb`,
`st_astext`, `st_asbinary`, `st_asewkb`, `st_srid`, `st_setsrid`, `st_intersects`, `st_contains`, and `st_within`.
Sedona Geography is not included. Install matching Sedona Python and JVM packages and its Spark extension. If the
selected helper or module is missing, Structure reports that exact dependency problem; it does not choose another
provider. These eleven helpers passed online/generated/native parity with Sedona 1.9.0 on Spark 3.5.3, 4.0.0, and
4.1.0, ordinary and Connect.

Sedona SRID metadata is preserved in schemas, including nested fields. An omitted constructor SRID and an SRID carried
inside a payload remain unknown to Structure. A Sedona predicate requires both operands to be Sedona Geometry with
the same fixed SRID.

## Provider coexistence boundary

Spark 4.1 and Sedona register overlapping unqualified `st_*` SQL function names. In a live coexistence run,
`SedonaSqlExtensions` changed Spark's native `st_geomfromwkb` result from the declared SRID-0 `GeometryType(0)` to
`GeometryType()` (mixed/unknown SRID). This makes the native contract fail schema validation and can change other
operations too. Keep Apache native and Sedona workloads in separate Spark sessions. An inline Binary bridge is not
currently supported because its source and destination expressions still share the colliding session registry.
WKB also drops SRID; preserve or restore SRID separately when transferring encoded values between sessions.

## Support boundaries

The default PySpark profile remains `>=3.5,<4.1`; Apache native helpers are not admitted there. Spark 4.1 native
helpers require `spark.sql.geospatial.enabled=true` and an Apache-only session. Ordinary Spark and Spark Connect are
separate execution modes. Databricks and Databricks Connect parity remains open until caller-owned sessions are
available. Streaming and JDBC/ODBC SQL warehouses are outside this batch PySpark DataFrame contract.

See the [design](../dev/design/Geospatial.design.md) and [specification](../dev/specifications/Geospatial.spec.md)
for implementation details and support boundaries.
