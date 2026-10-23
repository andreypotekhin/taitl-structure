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
| HLL aggregate union | `hll_union_agg(value, allow_different_lg_config_k=False, where=None)` | Nullable HLL state retaining declared precision |
| HLL composition | `hll_union(left, right, allow_different_lg_config_k=False)` | HLL state retaining precision |
| HLL consumption | `hll_sketch_estimate(value)` | Nullable Long estimate |
| Bitmap construction | `bitmap_construct_agg(value, where=None)` | Nullable opaque Bitmap state |
| Bitmap composition | `bitmap_or_agg(value, where=None)` | Nullable opaque Bitmap state |
| Bitmap consumption | `bitmap_count(value)` | Nullable Long count |
| Bitmap position helpers | `bitmap_bit_position(value)`, `bitmap_bucket_number(value)` | Nullable Long values |

HLL construction, HLL aggregate union, and Bitmap construction/OR are grouped metrics, so they change row grain.
Pairwise HLL union, HLL estimate, Bitmap count, and position helpers are scalar expressions and preserve one input row. Bitmap construction accepts Integer or
Long positions. HLL construction accepts a typed scalar expression and records a literal `lg_config_k` from 4 through
21.

## Precision and composition

HLL pairwise and aggregate union require equal `lg_config_k` precision by default. Passing
`allow_different_lg_config_k=True` is an explicit interoperability choice and emits `SKETCH-W0802`; Spark may reduce
the resulting precision. Aggregate union retains the declared precision in Structure's result type, while Spark
validates actual serialized inputs. Structure does not repair a runtime precision mismatch.

Bitmap OR accepts only branded Bitmap values. It does not admit raw Binary, a Python byte string, or an HLL value as a
substitute. HLL and Bitmap consumers similarly reject any value outside their declared opaque family.

## Storage, interchange, and profiles

The Binary representation is a Spark/profile implementation detail, not an interchange format. Persisted sketch state
must be read by a compatible Spark/provider profile, and applications own retention, migration, and compatibility
testing. Structure does not promise that state produced by a different Spark release, provider, or algorithm can be
read safely.

KLL and Theta share the opaque-state model and are admitted on exact ordinary and Connect `>=4.1,<4.2` after
batch/storage parity. KLL identity includes its Long/Float/Double domain; Theta identity records the algorithm. KLL
`k` and Theta `lg_nom_entries` configure operations rather than type identity. All four sketch families preserve their
logical identity recursively through `structure.logical_type` metadata. Untagged Binary needs an explicit declared
schema; conflicting or malformed metadata is rejected.

Count-Min state remains caller-owned. Typed batch observation metrics are a separate implemented exact-4.1 feature,
with their action lifecycle and snapshot model in the
[observation design](V11PySpark41ObservationsAndSketches.design.md#observation-boundary).

## Streaming and evidence

Scalar consumers and position helpers are row-preserving. Grouped construction and OR aggregation follow the existing
watermark, state, and output-mode rules for grouped streaming aggregates. Documentation must not imply that opaque
state has a portable streaming checkpoint or interchange guarantee.

Ordinary 4.1 KLL/Theta watermarked aggregation and restart with the same query/sketch configuration have positive
evidence. Connect support remains batch-only under the existing streaming policy.

Promotion or extension requires symbolic type/nullability validation, capability checks, online/generated rendering
parity, diagnostics, streaming classification, and target evidence. The authoritative specification is
[Sketches and Bitmaps Specification](../specifications/SketchBitmap.spec.md).
