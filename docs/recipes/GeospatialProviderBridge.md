# Native and Sedona Geometry Bridge: Current Gate

A Binary encoder and decoder are necessary to cross the native Spark Geometry and Sedona Geometry representations.
They are not currently sufficient to bridge them inside one Spark session.

Spark 4.1 and Sedona register overlapping unqualified `st_*` SQL function names. A live coexistence test with
`SedonaSqlExtensions` loaded showed that Spark's `st_geomfromwkb` registration was replaced: its result had
`GeometryType()` (mixed/unknown SRID) instead of the native helper's declared `GeometryType(0)`. Strict schema
validation then failed. Namespace-qualified Python calls cannot avoid that SQL registry collision in the current
runtime APIs.

Run native Spark and Sedona transformations in separate sessions/runtimes. When transferring values between them,
use a persisted Binary representation and preserve SRID separately; the source and destination must independently
validate their own schemas and semantics. WKB does not carry SRID. An inline cross-provider bridge remains gated until
both implementations can be invoked without the unqualified SQL function registry.

See the [Geospatial reference](../reference/Geospatial.ref.md#provider-coexistence-boundary) and implementation
[evidence plan](../dev/planning/P10192602.Geospatial-namespaces-and-native-types.plan.md).
