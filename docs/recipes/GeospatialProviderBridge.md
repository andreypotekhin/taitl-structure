# Cross-session Native and Sedona Geometry Transfer

A Binary encoder and decoder are necessary to cross the native Spark Geometry and Sedona Geometry representations.
They are not sufficient to bridge them inside one Spark session.

Spark 4.1 and Sedona register overlapping unqualified `st_*` SQL function names. A live coexistence test with
`SedonaSqlExtensions` loaded showed that Spark's `st_geomfromwkb` registration was replaced: its result had
`GeometryType()` (mixed/unknown SRID) instead of the native helper's declared `GeometryType(0)`. Strict schema
validation then failed. Namespace-qualified Python calls cannot avoid that SQL registry collision in the current
runtime APIs.

Structure emits `PYSPARK-W2707` when it finds Apache-native and Sedona helpers together in a Spark 4.1 transform.
Run native Spark and Sedona transformations in separate sessions. If values must cross that boundary, persist a
Binary representation and the SRID as separate columns. For example, with `source_session` configured for the source
provider and `native_session` configured for Apache Spark 4.1:

```python
import pyspark.sql.functions as F

handoff_path = "/data/spatial-handoff"

# Source session: encode and persist both the geometry and its SRID.
source_session.read.parquet("/data/source").select(
    F.expr("ST_AsBinary(shape)").alias("shape_wkb"),
    F.expr("ST_SRID(shape)").alias("shape_srid"),
).write.mode("overwrite").parquet(handoff_path)

# Destination session: restore the SRID after decoding WKB.
handoff = native_session.read.parquet(handoff_path)
restored = handoff.select(
    F.expr("ST_SetSRID(ST_GeomFromWKB(shape_wkb), shape_srid)").alias("shape")
)
```

Validate the source and destination schemas independently. WKB does not carry SRID, and this handoff is caller-owned;
Structure does not compile a cross-provider bridge inside one transform.

See the [Geospatial reference](../reference/Geospatial.ref.md#provider-coexistence-boundary) and implementation
[evidence plan](../dev/planning/P10192602.Geospatial-namespaces-and-native-types.plan.md).
