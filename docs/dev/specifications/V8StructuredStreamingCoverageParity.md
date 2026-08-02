# V8 Structured Streaming Coverage Parity

## Purpose

V8 raises PySpark Structured Streaming transformation coverage until it reaches the same support percentage as the
existing PySpark batch transformation catalog. Structure still returns transformed DataFrames only: callers own
`readStream`, `writeStream`, checkpoints, triggers, output modes, query start/stop, deployment, and recovery.

## Measurement

The current checked catalog is `src/structure/plugin/pyspark/resources/pyspark-transformation-coverage.json`. At v8
planning kickoff it contains thirty-six selected PySpark transformation families. Thirty-four are supported for batch,
so existing batch coverage is 34 / 36, or 94.4 percent. Thirty streaming-compatible rows are currently claimed, so
family-level streaming coverage is 30 / 36, or 83.3 percent. Among batch-supported families only, streaming coverage is
30 / 34, or 88.2 percent.

After the Sprint 37 stateless and ineligible-gate slices, typed array-of-struct generators and stream-stream
`union_all(...)` / `union_by_name(...)` are admitted, while arbitrary ordering and priority selection are explicit
streaming-ineligible rows. Raw family-level streaming coverage is 32 / 36, or 88.9 percent. Raw streaming coverage
among batch-supported families is 32 / 34, or 94.1 percent. Effective v8 parity coverage is 32 / 32, or 100.0 percent,
after excluding the two explicit Spark-ineligible families from the denominator.

V8 parity means the streaming percentage must be no lower than the batch percentage for the same measured catalog. If a
family is impossible or unsafe on streaming DataFrames for the supported PySpark 3.5.x and 4.0.x targets, v8 must record
that family as streaming-ineligible with evidence and remove it from the streaming denominator. A streaming-ineligible
family is not a hidden gap; it is an explicit Spark limitation or an explicit Structure non-goal.

## Required Ledger

V8 adds a checked streaming coverage ledger beside the existing catalog. The ledger must classify every supported batch
family as one of:

- `streaming-supported`, meaning online and generated execution accept caller-supplied streaming DataFrames and live
  restart evidence passes on PySpark 3.5 and 4.0;
- `streaming-partial`, meaning at least one public operation in the family is supported but another operation in the
  same batch family is Spark-ineligible or still batch-only;
- `streaming-ineligible`, meaning Spark or Structure cannot support the shape without violating caller-owned streaming;
- `streaming-deferred`, meaning the family is plausible but lacks the v8 design, diagnostics, and live evidence.

Families marked `streaming-partial` must also be split into operation-level rows. The operation-level rows are the
actual numerator and denominator for parity whenever family-level accounting would hide Spark's restrictions. For
example, `union_all(...)` may be evaluated separately from `intersect(...)`, and post-aggregate complete-mode
`order_by(...)` may be evaluated separately from arbitrary `limit(...)`.

## Initial Gap Candidates

At kickoff the batch-supported families still marked batch-only for streaming are:

- `functions.generators`: typed array-of-struct row expansion helpers such as `explode_struct(...)` and
  `inline_struct(...)`;
- `dataframe.set`: exact-schema set operations, where union-like operations may be streaming candidates but distinct
  and subtract/intersect shapes require Spark-specific rejection evidence;
- `dataframe.ordering`: ordering, limits, and offsets, where arbitrary streaming ordering is ineligible and only a
  narrow post-aggregate complete-mode order may be considered;
- `dataframe.priority-selection`: `select_first_qualified(...)`, which currently lowers through row-number style
  selection and needs a streaming-specific state contract before admission.

V8 must not broaden these by optimism. Each candidate starts as unsupported for streaming until a design gate describes
state, output-mode, cardinality, generated source, diagnostics, and restart evidence.

The first stateless design gate admits `functions.generators` as row-expanding but stateless, and splits
`dataframe.set`: exact-schema `union_all(...)` and `union_by_name(...)` are supported only when both relation inputs
are declared with `streaming=True`; `intersect(...)`, `intersect_all(...)`, `subtract(...)`, and `except_all(...)`
remain streaming-ineligible.

The ordering and priority-selection design gates are closed as ineligible for v8. `order_by(...)`, `limit(...)`, and
`offset(...)` require a batch materialization boundary for unbounded streaming relations. `select_first_qualified(...)`
remains batch-only because it lowers through ranking and validation aggregates.

## Admission Rules

All admitted streaming transformations must preserve caller-owned lifecycle. Generated source and online execution must
not call `readStream`, `writeStream`, `start`, `awaitTermination`, checkpoint configuration, trigger configuration,
output-mode configuration, Spark actions, RDD conversion, Pandas conversion, or hidden Python UDF fallback.

V8 inherits the v7 baseline: stream-static enrichment, stream-static left-outer lookup, and one admitted stateful
operation followed only by stateless transformations are already proven caller-owned streaming shapes. V8 measures,
extends, or rejects the remaining catalog gaps without weakening that boundary.

Stateless row-local operations may be admitted when they lower to ordinary Spark Column or DataFrame transformations
that Spark accepts on streaming DataFrames. Stateful operations require a compiler-visible state boundary, a watermark
or finite bound where Spark requires one, a documented output-mode rule, explain output, and diagnostics for unsafe
composition.

V8 keeps Spark Connect streaming out of scope. Spark Connect batch support remains independent of classic PySpark
Structured Streaming claims.

## Acceptance

V8 is complete when the streaming ledger passes a guard test proving its denominator and numerator, the streaming
percentage is at least the current batch percentage, every supported streaming operation has online/generated parity and
live PySpark 3.5/4.0 restart evidence, and every rejected operation fails before query start with a corrective
diagnostic.
