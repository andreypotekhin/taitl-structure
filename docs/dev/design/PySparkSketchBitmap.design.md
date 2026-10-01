# PySpark Sketch and Bitmap Design

## Purpose

This design defines Structure's documented contract for the default-baseline HyperLogLog (HLL) and Bitmap functions.
It refines the sketch boundary in [PySpark SQL Boundary Contracts](PySparkSQLBoundaryContracts.design.md) into a
separate implementation and public-documentation lane. The lane exists because sketch state crosses schema, aggregate,
scalar-expression, capability, persistence, and streaming boundaries; it is not merely another aggregate alias.

## Design decision

HLL and Bitmap are first-class opaque Structure types. Spark stores both as Binary, but Structure retains an algorithm
brand so an HLL cannot be passed to a Bitmap consumer, cast to ordinary Binary, or silently exchanged with a different
sketch family. A declared `hll_sketch(lg_config_k=...)` field also retains its precision parameter in the compiled
schema contract.

The public documentation uses dedicated Sketches and Bitmaps pages rather than an `Other` section. The APIs span the
Schemas, Aggregations, and Expressions surfaces; the ownership, precision, and persistence rules would be difficult to
discover in any one of those inventories. Those existing pages link to the dedicated pages and retain compact tables
for their own local symbols.

## Baseline surface

The default PySpark `>=3.5,<4.1` surface contains:

| Family | Structure surface | Result |
| --- | --- | --- |
| HLL construction | `hll_sketch_agg(value, lg_config_k=12, where=None)` | Nullable opaque HLL state |
| HLL composition | `hll_union(left, right, allow_different_lg_config_k=False)` | HLL state retaining precision |
| HLL consumption | `hll_sketch_estimate(value)` | Nullable Long estimate |
| Bitmap construction | `bitmap_construct_agg(value, where=None)` | Nullable opaque Bitmap state |
| Bitmap composition | `bitmap_or_agg(value, where=None)` | Nullable opaque Bitmap state |
| Bitmap consumption | `bitmap_count(value)` | Nullable Long count |
| Bitmap position helpers | `bitmap_bit_position(value)`, `bitmap_bucket_number(value)` | Nullable Long values |

All aggregate forms are grouped metrics. The scalar HLL/Bitmap consumers preserve one input row. Bitmap construction
accepts Integer or Long positions. HLL construction accepts a typed scalar expression and records a literal
`lg_config_k` in the inclusive range 4 through 21.

## Precision and composition

HLL union requires equal `lg_config_k` precision by default. Passing
`allow_different_lg_config_k=True` is an explicit interoperability choice and emits `SKETCH-W0802`; Spark may reduce
the resulting precision. Structure does not infer or repair a runtime precision mismatch.

Bitmap OR accepts only branded Bitmap values. It does not admit raw Binary, a Python byte string, or an HLL value as a
substitute. HLL and Bitmap consumers similarly reject any value outside their declared opaque family.

## Storage, interchange, and profiles

The Binary representation is a Spark/profile implementation detail, not an interchange format. Persisted sketch state
must be read by a compatible Spark/provider profile, and applications own retention, migration, and compatibility
testing. Structure does not promise that state produced by a different Spark release, provider, or algorithm can be
read safely.

KLL and Theta type declarations share the opaque-state model but require the PySpark 4.1 capability profile and live
evidence. They are not default-baseline aggregate support. Count-Min state and observation metrics remain
caller-owned because the current typed surface lacks their complete consumer and side-channel contracts.

## Streaming and evidence

Scalar consumers and position helpers are row-preserving. Grouped construction and OR aggregation follow the existing
watermark, state, and output-mode rules for grouped streaming aggregates. Documentation must not imply that opaque
state has a portable streaming checkpoint or interchange guarantee.

Promotion or extension requires symbolic type/nullability validation, capability checks, online/generated rendering
parity, diagnostics, streaming classification, and target evidence. The authoritative specification is
[PySpark Sketch and Bitmap Specification](../specifications/PySparkSketchBitmap.spec.md).
