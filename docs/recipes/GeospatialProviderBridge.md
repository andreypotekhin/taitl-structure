# Bridge a Provider-Specific Geometry Payload

**Problem:** A geometry producer needs to send coordinates to another step or system without sharing its provider's
physical Geometry type.

**Solution:** Encode an agreed binary format, carry it through an ordinary `binary()` field, and decode it explicitly
at the receiver.

## Choose the representation and runtime

This example starts with WKT coordinates in SRID 4326 and uses Sedona SQL to produce and consume EWKB. EWKB includes
SRID information; the application still owns agreement on the codec, geometry kinds, dimensions, and provider
versions. See [Sedona's geometry functions](https://sedona.apache.org/latest/api/sql/Geometry-Functions/) for its
native constructors and serializers.

The caller configures a Spark session with compatible Sedona dependencies and registered SQL functions. Every `ST_*`
call below must resolve to Sedona in that session. Structure's provider namespaces remain target-gated, so these
examples use the existing typed `sql(...)` interface and ordinary String/Binary fields. Structure does not install
Sedona or verify that another provider can decode the resulting bytes.

| Handoff | Representation | Receiver responsibility |
| --- | --- | --- |
| Sedona to a Sedona consumer | EWKB | Decode EWKB and verify coordinates and SRID |
| Standard WKB interchange | WKB plus an SRID column | Decode WKB and restore the agreed SRID |
| Another provider or external system | Agreed codec and metadata | Validate the exact producer/receiver pair |

## Define the input and binary output

The identifier connects each payload to its source row. The Binary field carries bytes; it does not declare a geometry
codec. Nullable output fields accommodate provider SQL results, so the receiving application must decide how to
handle missing or rejected payloads:

```python
from structure import *
from structure.plugin.pyspark import *


class RawBoundary(Schema):
    id = string(nullable=False)
    wkt = string(nullable=False)


class EncodedBoundary(Schema):
    id = string(nullable=False)
    payload = binary()


class DecodedBoundary(Schema):
    id = string(nullable=False)
    wkt = string()
    srid = integer()
```

## Encode EWKB through typed SQL

The provider parses WKT with an explicit SRID and serializes the geometry within the same SQL statement. The
Structure output contains only the identifier and binary payload:

```python
class EncodeBoundary(Transform):
    source = input(RawBoundary)
    encoded = output(EncodedBoundary)

    def encode(self, row: RawBoundary) -> EncodedBoundary:
        return sql(
            "SELECT id, ST_AsEWKB(ST_GeomFromWKT(wkt, 4326)) AS payload FROM {source}",
            relations={"source": row},
            to=EncodedBoundary,
        )
```

The source coordinates must already use SRID 4326. Assigning an SRID labels the coordinate reference system;
coordinate conversion requires an explicit provider transformation.

## Decode at the receiving step

A receiving Sedona step can decode the agreed EWKB and expose ordinary fields for inspection:

```python
class DecodeBoundary(Transform):
    encoded = input(EncodedBoundary)
    decoded = output(DecodedBoundary)

    def decode(self, row: EncodedBoundary) -> DecodedBoundary:
        return sql(
            "SELECT id, ST_AsText(shape) AS wkt, ST_SRID(shape) AS srid "
            "FROM (SELECT id, ST_GeomFromEWKB(payload) AS shape FROM {encoded}) received",
            relations={"encoded": row},
            to=DecodedBoundary,
        )
```

A different provider or external system must use its own verified decoder. A Binary input alone does not establish
EWKB support, SRID preservation, or compatibility with every geometry kind.

## Run and check a round trip

Use a small fixture with a declared Spark schema, then compare the decoded coordinates and SRID:

```python
from pyspark.sql import types as T


source_df = spark.createDataFrame(
    [("point-1", "POINT (1 2)")],
    T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("wkt", T.StringType(), False),
    ]),
)
session = StructureSession(spark=spark)
encoded = EncodeBoundary(source=source_df).run(session).encoded
decoded = DecodeBoundary(encoded=encoded).run(session).decoded
point = decoded.first()
assert (point.id, point.wkt, point.srid) == ("point-1", "POINT (1 2)", 4326)
```

Composition can perform the same handoff within a pipeline:

```python
pipeline = EncodeBoundary(source=source_df).to(DecodeBoundary())
decoded = pipeline.run(session).decoded
```

Spark evaluates the SQL lazily; `first()` above forces execution for the fixture. Extend the application test with its
actual polygons, empty geometries, dimensions, and malformed inputs, and execute it with the receiver's decoder.
Compare geometry meaning rather than byte identity or WKT formatting across providers. A producer-only round trip
does not prove that an external consumer understands the payload.

## Carry SRID separately with standard WKB

For a receiver that expects standard WKB, project a separate SRID column:

```python
class WkbBoundary(Schema):
    id = string(nullable=False)
    payload = binary()
    srid = integer()


class EncodeWkbBoundary(Transform):
    source = input(RawBoundary)
    encoded = output(WkbBoundary)

    def encode(self, row: RawBoundary) -> WkbBoundary:
        return sql(
            "SELECT id, ST_AsBinary(shape) AS payload, ST_SRID(shape) AS srid "
            "FROM (SELECT id, ST_GeomFromWKT(wkt, 4326) AS shape FROM {source}) parsed",
            relations={"source": row},
            to=WkbBoundary,
        )


class DecodeWkbBoundary(Transform):
    encoded = input(WkbBoundary)
    decoded = output(DecodedBoundary)

    def decode(self, row: WkbBoundary) -> DecodedBoundary:
        return sql(
            "SELECT id, ST_AsText(shape) AS wkt, ST_SRID(shape) AS srid "
            "FROM (SELECT id, ST_SetSRID(ST_GeomFromWKB(payload), srid) AS shape "
            "FROM {encoded}) received",
            relations={"encoded": row},
            to=DecodedBoundary,
        )
```

Standard WKB does not encode the SRID. The receiver restores the supplied label explicitly; this step does not
transform coordinates. Choose the codec at the application protocol level rather than guessing it from each payload.

## Keep spatial values typed within one provider

Use a binary handoff for an external protocol, persisted interchange format, or provider transfer. Within a supported
provider-scoped spatial pipeline, keep Geometry or Geography typed so provider, kind, and SRID checks remain useful.

Record the codec, producer and receiver versions, SRID rules, dimensionality, and null/error policy in the owning
application. Structure does not insert a decoder, set an SRID, or convert coordinates on that application's behalf.
See the [geospatial reference](../reference/Geospatial.ref.md) and
[background](../background/Geospatial.back.md) for the provider model and current target limits.
