# Design: Geospatial Namespaces and Representations

## Purpose

Spatial APIs vary by Python package, physical Spark type, and server runtime. Structure makes those choices visible in
the helper namespace and field declaration. Spark and Sedona register overlapping unqualified SQL names, so their
helpers currently run in separate sessions. This
design removes ambient provider selection and keeps managed-runtime support tied to evidence from the actual runtime.

The implementation record is [P10192602](../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md).

## Namespace selects the helper

Root `st_*` functions call Apache PySpark. `databricks.st_*` calls
`pyspark.databricks.sql.functions`. `sedona.st_*` calls Sedona's public Python wrappers. The source spelling selects
the implementation on every expression; no `geo_provider` configuration, decorator option, precedence rule, or
automatic distribution detection exists. The generated and online evaluators dispatch from this stored namespace and
never fall back to a different package.

## Field type selects the representation

`geometry(srid=None)` and `geography(srid=None)` declare native Spark values. Databricks values use the same native
declarations where the runtime constructors produce compatible physical types. Sedona uses
`sedona.geometry(srid=None)` and a distinct Geometry UDT. Helper provenance therefore does not imply a different
physical type, and a shared SQL type does not imply that every runtime exposes every helper.

`None` records mixed or unknown SRID; an integer records a fixed SRID. Native physical constructors preserve SRID.
Sedona's UDT does not consistently preserve the logical SRID in the Python schema, so
`structure.logical_type` metadata carries it recursively through arrays, maps, and structs. Missing metadata remains
caller-declared intent; present conflicting metadata fails. This declaration does not inspect input rows.

## Bridges and semantic boundaries

The initial helper catalog is intentionally small. Native operations cover WKB construction, WKB encoding, SRID
inspection, and SRID assignment. Databricks exposes the same family with its optional `st_geomfromwkb` SRID argument.
Sedona adds WKT/EWKB constructors, text/binary encoders, SRID operations, and three predicates. No spatial joins,
measurements, Geography support in Sedona, or coordinate transformations are included. H3 and S2 remain separate
cell-index APIs rather than Geometry/Geography representations.

Binary encoding is the required representation boundary for any future cross-provider bridge, but it does not resolve
the current SQL registration collision. A live Spark 4.1 test with `SedonaSqlExtensions` loaded shows that
`st_geomfromwkb` resolves to a different function and returns a mixed-SRID type instead of Spark's SRID-0 type. Inline
Apache/Sedona bridges and mixed-provider queries remain gated until both implementations can be selected without
unqualified SQL name lookup. WKB loses SRID; the caller must restore it separately. Structure does not infer
semantics from serialized bytes or silently cast values.

## Admission and managed runtimes

Native schema constructors are checked at runtime on exact Spark 4.0 and 4.1 profile declarations. This conditional
check allows a managed distribution to backport the types without claiming they exist in stock Apache Spark 4.0.
Apache helpers remain exact 4.1 capabilities. Databricks helpers have independent exact 4.0 and 4.1 gates plus actual
module/function checks. Sedona has its own package, profile, and execution-mode gates. A package import check is not
parity evidence.

Spark-free compilation remains possible because optional packages load only when their value representation or
helper is used. Online/generated results, schemas, nulls, malformed inputs, and namespace collisions must match the
selected upstream runtime. Ordinary and Connect modes are separately evidenced. Managed Databricks Spark and
Databricks Connect need their own sessions and results; local Apache Spark cannot certify them. Batch evidence does
not establish streaming support.
