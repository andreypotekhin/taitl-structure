# Design: Sketches and Bitmaps

## Purpose

HLL and Bitmap support lets a transform publish compact analytical state instead of only a final count. An HLL carries
an approximate distinct-cardinality summary; a Bitmap carries a compact set of integral positions. Both are useful
only when a later transform can understand the state that an earlier transform produced.

This design defines that shared state boundary for the default PySpark `>=3.5,<4.1` baseline. It refines the opaque
state discussion in [PySpark SQL Boundary Contracts](PySparkSQLBoundaryContracts.design.md) into an implementable and
user-facing feature. It does not attempt to admit every Spark sketch or to present Spark Binary values as a general
serialization format.

## Design stance

Sketch state is a typed value, not an implementation-shaped `binary()` field. Spark stores both values as Binary, but
Structure retains an algorithm brand so an HLL cannot be passed to a Bitmap consumer, cast to ordinary Binary, or
silently exchanged with another sketch family. A declared `hll_sketch(lg_config_k=...)` field also retains its
precision parameter in the compiled schema contract.

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

The construction and Bitmap OR forms are grouped metrics, so they change row grain. HLL union, HLL estimate, Bitmap
count, and position helpers are scalar expressions and preserve one input row. Bitmap construction accepts Integer or
Long positions. HLL construction accepts a typed scalar expression and records a literal `lg_config_k` from 4 through
21.

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
[Sketches and Bitmaps Specification](../specifications/SketchBitmap.spec.md).
