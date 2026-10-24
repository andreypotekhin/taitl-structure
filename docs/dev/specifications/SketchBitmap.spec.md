# Sketches and Bitmaps Specification

## Scope

This specification covers the baseline HLL and Bitmap design for PySpark `>=3.5,<4.2` and the exact PySpark 4.1 KLL
and Theta extension. It covers opaque schema fields, typed operations, diagnostics, rendering, recursive metadata,
and streaming classification. It does not claim a portable Binary serialization format, Count-Min support, or
observation metrics.

The governing design is [Sketches and Bitmaps](../design/SketchBitmap.design.md). The broader
boundary rationale remains in [PySpark SQL Boundary Contracts](../design/PySparkSQLBoundaryContracts.design.md).

## Declaring state

`hll_sketch(lg_config_k=12)` declares HLL with precision 4 through 21; `bitmap()` declares Bitmap. Both materialize to
Spark Binary with logical identity in `structure.logical_type` metadata. KLL declarations require `kll_sketch(long())`,
`kll_sketch(float())`, or `kll_sketch(double())`. `theta_sketch()` declares the single Theta family. Sketch identity
is not assignable to ordinary Binary or another family/domain.

A Schema field may carry a sketch across compiled transform and supported storage boundaries. Untagged Binary remains
Binary unless an explicit Schema declares sketch intent. Explicit declarations can supply missing metadata but cannot
override malformed, unsupported-version, or conflicting metadata. Metadata does not validate serialized bytes. The
former `profile=` factory argument is rejected; configure `plugin.pyspark.profile` instead.

## Operations

| Function | Input contract | Result contract |
| --- | --- | --- |
| `hll_sketch_agg` | Scalar; literal `lg_config_k` 4--21; optional `where` | Nullable HLL preserving precision |
| `hll_union_agg` | HLL value; optional `allow_different_lg_config_k` and `where` | Nullable HLL preserving declared precision; mixed-precision opt-in warns that Spark may reduce precision |
| `hll_union` | Two HLL values | Nullable HLL; equal precision by default |
| `hll_sketch_estimate` | HLL value | Nullable Long |
| `bitmap_construct_agg` | Integer or Long expression; optional Boolean `where` | Nullable Bitmap |
| `bitmap_or_agg` | Bitmap value; optional Boolean `where` | Nullable Bitmap |
| `bitmap_count` | Bitmap value | Nullable Long |
| `bitmap_bit_position`, `bitmap_bucket_number` | Integer or Long expression | Nullable Long |
| `kll_sketch_agg`, `kll_sketch_get_n`, `kll_sketch_get_quantile`, `kll_sketch_get_rank`, `kll_sketch_merge`, `kll_sketch_to_string` | Exact PySpark 4.1 ordinary/Connect; Long/Float/Double domain | Domain-preserving KLL, Long count, Double rank, String rendering |
| `theta_sketch_agg`, `theta_union`, `theta_union_agg`, `theta_intersection`, `theta_intersection_agg`, `theta_difference`, `theta_sketch_estimate` | Exact PySpark 4.1 ordinary/Connect; native supported input types | Theta state or Long estimate |

`hll_union(..., allow_different_lg_config_k=True)` and
`hll_union_agg(..., allow_different_lg_config_k=True)` are mixed-precision exceptions. Each emits `SKETCH-W0802` and
records the opt-in in expression metadata. Pairwise mismatches fail during symbolic authoring by default; the
aggregate form retains the declared input precision and Spark validates runtime sketch compatibility.

## Lowering and diagnostics

Each helper creates an explicitly typed symbolic aggregate or call expression. Generated and online PySpark use the
matching public `pyspark.sql.functions` name. Capability requirements are `schema.sketches` for declared types and
`expression.sketches` for expressions and aggregates.

KLL construction `k` defaults to 200 (8–65535); Theta `lg_nom_entries` defaults to 12 (4–26). These parameters are
operation configuration and are excluded from logical type identity. KLL generic helpers dispatch by declared
domain. Spark 4.1.0 supplies pairwise KLL merge but no merge aggregate. Empty KLL rank/quantile calls preserve the
native error; guard with `kll_sketch_get_n(...) > 0` when needed. Theta accepts Spark's native scalar types and
integral arrays without Structure conversion.

Type errors must identify the required opaque kind or integral input. The mixed-precision warning must state that the
caller intentionally permits a precision trade-off. Diagnostic text must never render opaque-state payload values.

## Streaming classification

Pairwise sketch operations and scalar consumers are row-local. HLL/Bitmap retain their existing streaming
classification. Ordinary PySpark 4.1 KLL/Theta aggregate parity and same-configuration checkpoint restart are
evidenced for watermarked grouped streams. Spark Connect remains outside streaming support under the existing backend
boundary. No helper owns checkpoint state, sinks, output modes, or queries.

## Acceptance

The implementation is acceptable when focused tests prove all of the following:

- HLL and Bitmap types preserve their brands and materialize as Binary;
- invalid precision, raw Binary, opposite-family state, and non-integral Bitmap inputs fail before Spark runs;
- HLL construction, pairwise/aggregate union, estimate, Bitmap construction/OR/count, and position helpers render to the public PySpark
  calls and agree in online execution;
- mixed HLL precision rejects by default and emits `SKETCH-W0802` when explicitly permitted;
- schema/profile capability checks reject KLL/Theta outside their target profile; and
- the catalog, streaming ledgers, API pages, background, reference, recipe, and Quick Reference describe the same
  baseline and caller-owned boundaries.
