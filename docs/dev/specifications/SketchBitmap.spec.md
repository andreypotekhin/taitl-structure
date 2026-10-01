# Sketches and Bitmaps Specification

## Scope

This specification makes the baseline HLL and Bitmap design executable for ordinary PySpark `>=3.5,<4.1`. It covers
declared opaque schema fields, aggregate construction/composition, scalar consumers, diagnostics, rendering, and
streaming classification. It does not claim a portable Binary serialization format, KLL/Theta aggregate support,
Count-Min support, or observation metrics.

The governing design is [Sketches and Bitmaps](../design/SketchBitmap.design.md). The broader
boundary rationale remains in [PySpark SQL Boundary Contracts](../design/PySparkSQLBoundaryContracts.design.md).

## Declaring state

`hll_sketch(lg_config_k=12)` declares an HLL value with a literal precision from 4 through 21; `bitmap()` declares a
Bitmap value. Both materialize to Spark Binary while retaining their Structure algorithm brand and profile metadata.
They are not assignable to ordinary Binary or to another sketch family.

A Schema field may carry HLL or Bitmap state across a compiled transform boundary. A raw Binary field cannot enter a
typed sketch consumer without a separately admitted import contract. `kll_sketch()` and `theta_sketch()` require the
PySpark 4.1 profile capability and remain outside this baseline function surface.

## Operations

| Function | Input contract | Result contract |
| --- | --- | --- |
| `hll_sketch_agg` | Scalar; literal `lg_config_k` 4--21; optional `where` | Nullable HLL preserving precision |
| `hll_union` | Two HLL values | Nullable HLL; equal precision by default |
| `hll_sketch_estimate` | HLL value | Nullable Long |
| `bitmap_construct_agg` | Integer or Long expression; optional Boolean `where` | Nullable Bitmap |
| `bitmap_or_agg` | Bitmap value; optional Boolean `where` | Nullable Bitmap |
| `bitmap_count` | Bitmap value | Nullable Long |
| `bitmap_bit_position`, `bitmap_bucket_number` | Integer or Long expression | Nullable Long |

`hll_union(..., allow_different_lg_config_k=True)` is the sole mixed-precision exception. It emits `SKETCH-W0802` and
records the opt-in in the expression metadata. Any other HLL precision mismatch fails during symbolic authoring.

## Lowering and diagnostics

Each helper creates an explicitly typed symbolic aggregate or call expression. Generated and online PySpark use the
matching public `pyspark.sql.functions` name. Capability requirements are `schema.sketches` for declared types and
`expression.sketches` for expressions and aggregates.

Type errors must identify the required opaque kind or integral input. The mixed-precision warning must state that the
caller intentionally permits a precision trade-off. Diagnostic text must never render opaque-state payload values.

## Streaming classification

`hll_union`, `hll_sketch_estimate`, `bitmap_bit_position`, `bitmap_bucket_number`, and `bitmap_count` are row-local.
`hll_sketch_agg`, `bitmap_construct_agg`, and `bitmap_or_agg` are grouped aggregates and follow the ordinary
watermark/state/output-mode rules. No helper grants an ownership claim over checkpoint state, sinks, or output modes.

## Acceptance

The implementation is acceptable when focused tests prove all of the following:

- HLL and Bitmap types preserve their brands and materialize as Binary;
- invalid precision, raw Binary, opposite-family state, and non-integral Bitmap inputs fail before Spark runs;
- HLL construction, union, estimate, Bitmap construction/OR/count, and position helpers render to the public PySpark
  calls and agree in online execution;
- mixed HLL precision rejects by default and emits `SKETCH-W0802` when explicitly permitted;
- schema/profile capability checks reject KLL/Theta outside their target profile; and
- the catalog, streaming ledgers, API pages, background, reference, recipe, and Quick Reference describe the same
  baseline and caller-owned boundaries.
