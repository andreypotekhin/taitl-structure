# Geospatial Specification

## Status

This specification replaces the experimental provider-neutral Geometry API. A helper's Python namespace selects its
implementation; a field type selects the physical spatial representation. No application, transform, or step setting
selects a provider. Each helper, schema type, dependency, and execution mode remains gated until its parity evidence is
recorded in the compatibility docs.

## Public types

The root PySpark namespace declares native Spark types:

    geometry(srid=None, **field_options)
    geography(srid=None, **field_options)

`Geometry`/`GeometryType` and `Geography`/`GeographyType` are public type constructors. `None` means mixed or unknown
SRIDs; an integer means fixed SRID. Reject booleans and non-integers. Spark validates whether a declared integer SRID
is in its supported spatial reference catalog. Native constructors materialize as `T.GeometryType(srid)` and
`T.GeographyType(srid)`, using `"ANY"` for mixed SRIDs. Constructor availability is checked at materialization and
generated-schema import because managed Spark distributions can backport these types.

Sedona Geometry has a separate declaration and physical UDT:

    sedona.geometry(srid=None, **field_options)
    sedona.Geometry / sedona.GeometryType

Its fixed SRID is stored in `structure.logical_type` metadata, including when nested in arrays, maps, or structs. A
missing descriptor is treated as caller-declared input intent; conflicting or malformed descriptors are rejected.
No per-row SRID scan is performed.

## Namespaced helpers

Root helpers call the matching Apache PySpark function and are admitted only for exact `>=4.1,<4.2` profiles:

    st_geomfromwkb(value)
    st_geogfromwkb(value)
    st_asbinary(value)
    st_srid(value)
    st_setsrid(value, srid)

WKB constructors take Binary expressions. Apache Geometry construction has SRID 0; Geography construction has SRID
4326. `st_asbinary` returns Binary, `st_srid` returns Integer, and `st_setsrid` keeps the kind and representation.
A literal SRID yields a fixed-SRID type; a symbolic integral expression yields mixed SRID.

Spark 4.1 requires the caller to enable `spark.sql.geospatial.enabled=true` in the Spark session before native spatial
queries execute. Structure does not set this Spark configuration.

Databricks helpers are called through `databricks.*` and resolve only to
`pyspark.databricks.sql.functions`. They have independent runtime requirements; `databricks.st_geomfromwkb(value,
srid=None)` includes Databricks' optional SRID argument. Databricks calls do not request Apache helper capabilities.

Sedona helpers are called through `sedona.*` and use Sedona's public Python APIs:

    sedona.st_geomfromwkt(value, srid=None)
    sedona.st_geomfromwkb(value)
    sedona.st_geomfromewkb(value)
    sedona.st_astext(value)
    sedona.st_asbinary(value)
    sedona.st_asewkb(value)
    sedona.st_srid(value)
    sedona.st_setsrid(value, srid)
    sedona.st_intersects(left, right)
    sedona.st_contains(left, right)
    sedona.st_within(left, right)

The first slice does not add Sedona Geography or its broader spatial catalog. An omitted constructor SRID and an
encoded SRID remain unknown to Structure; compilation does not inspect payloads. Each helper preserves its upstream
null and malformed-input behavior.

## Representation and bridges

Apache and Databricks helpers consume and produce the shared native Geometry/Geography types when the runtime's
physical constructors are compatible. Sedona helpers consume Sedona values. Spatial predicates require identical
representation, kind, and the same fixed SRID. Unsupported spatial comparisons, arithmetic, casts, and implicit
conversions fail during authoring or compilation instead of reaching generic scalar handling.

Spark and Sedona register overlapping unqualified `st_*` SQL names. Live Spark 4.1 tests show that loading
`SedonaSqlExtensions` replaces Spark's `st_geomfromwkb` registration: the native helper then returns
`GeometryType()` instead of its declared `GeometryType(0)`. A single Spark session therefore cannot currently mix
Apache native and Sedona helpers or use an inline Binary bridge between them. Run native and Sedona workloads in
separate sessions/runtimes. A Binary boundary alone does not avoid this SQL registration collision. WKB also drops
SRID, and Structure performs no coordinate transformation.

## Capability and evidence rules

The default profile remains `>=3.5,<4.1`. Native type constructors are conditionally available on exact
`>=4.0,<4.1` and `>=4.1,<4.2` profiles, after checking the actual runtime constructor. Apache helpers are exact 4.1
only. Databricks helpers have separate exact 4.0/4.1 requirements and require actual module and function checks.
Sedona has independent dependency, profile, and mode requirements. No provider fallback is permitted.

Spark-free compilation does not import PySpark, Databricks, or Sedona. Online execution and generated source dispatch
only to the named namespace and report missing dependencies with a link to the Geospatial reference. Sedona 1.9.0
parity for its eleven helpers passes on Spark 3.5.3, 4.0.0, and 4.1.0 in ordinary and Connect modes, with Sedona's
server package/extension enabled in an isolated process. Apache native parity passes on ordinary and Connect Spark
4.1.0 only in an Apache-only session with `spark.sql.geospatial.enabled=true`. Managed Databricks Spark and
Databricks Connect evidence remains open; an Apache image or mocked module does not establish Databricks parity.
Batch evidence makes no streaming claim.
