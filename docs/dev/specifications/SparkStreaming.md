# Spark Streaming Transformation Support

This specification defines Spark Structured Streaming support for Structure transforms. Structure compiles DataFrame
transformations that may run on streaming DataFrames; callers own streaming lifecycle code in every release.

## Support Claim

Structure supports a transform with a streaming input when all of these are true:

- the configured backend is PySpark;
- the caller creates the streaming source and passes the streaming DataFrame into execution or generated-code execution;
- the transform has one current pipeline input that may be streaming;
- joined side inputs are static DataFrames;
- every compiler-visible operation is classified as streaming-compatible;
- opaque hooks are absent or explicitly marked `streaming=True`;
- generated-code execution and execution do not emit or call streaming lifecycle APIs, Spark actions, RDD conversion, Pandas
  conversion, Python UDFs, or local collection.

The support claim covers returned DataFrame plans only. Streaming sources, sinks, query start/stop behavior, triggers,
checkpoints, query names, deployment, and recovery remain caller-owned.

## Configuration

The existing configuration key remains:

```toml
[tool.structure]
streaming_compatibility_checks = true
```

Transform-level opt-in remains:

```python
@transform(streaming=True)
class EnrichOrders(Transform):
    ...
```

Severity rules:

- if checks are disabled and no transform opts in, no streaming diagnostics are emitted;
- if checks are enabled and the transform does not opt in, incompatible operations are warnings;
- if the transform opts in, incompatible and unknown operations are errors, even when global checks are disabled.

## Runtime Contract

Execution uses the existing session:

```python
session = StructureSession(spark=spark, ctx=ctx, config=config)
result = EnrichOrders(orders=orders_stream, customers=customers_static).run(session)
```

Generated-code execution uses the existing generated class API:

```python
result = EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
    orders=orders_stream,
    customers=customers_static,
)
```

For a compatible transform, `result.isStreaming` should be true when the current input is streaming. Structure uses a
single narrow `isStreaming` branch only for ordinary `drop_duplicates(...)`, selecting batch `dropDuplicates` or
streaming `dropDuplicatesWithinWatermark`; all lifecycle behavior remains caller-owned.

## Supported Operations

The first slice supports these operation classes:

```text
streaming.row_local_projection
streaming.row_local_filter
streaming.schema_only_validation
streaming.watermark
streaming.stream_static_left_join
streaming.stream_static_inner_join
streaming.streaming_hook_boundary
```

Projection and filtering are compatible only when every expression lowers to Spark Column operations that do not need
cross-row state, actions, UDFs, RDD conversion, Pandas conversion, or local collection.

Schema-only validation may inspect DataFrame schema metadata, column names, Spark data types, and nullability metadata.
It must not trigger a Spark job.

Stream-static joins are compatible only for left and inner joins where the current pipeline side may be streaming and
the joined side is static. Static-side broadcast hints are allowed when already supported by the PySpark target.

Hooks are compatible only when the hook is marked `streaming=True`. The checker treats this as a trusted boundary,
not as proof from body analysis.

`input(..., streaming=True | False)` records whether a transform input is explicitly streaming. The default
`False` treats joined side inputs as static unless the author declares them streaming.

`watermark(field, delay="10 minutes")` records a compiler-visible event-time watermark and lowers to PySpark
`DataFrame.withWatermark(...)`. Watermarks are transformation metadata, not lifecycle ownership.
Execution interprets that lowered recipe directly, and generated-code execution renders the same call. A watermark on
a lookup relation is applied before that relation is joined.

`event_time_between(left_time, right_time, upper=..., lower="0 seconds")` records the bounded event-time relationship
required for supported stream-stream joins and lowers to a Spark-visible boolean predicate.

Grouped aggregations and exact/subset dedupe are streaming-compatible when a watermark appears earlier in the same
step method on the current streaming frame. Explain output reports the Spark output modes the caller must use, but
Structure never calls `writeStream.outputMode(...)`.

An inner `rowset_join(...)` between two inputs declared `streaming=True` is compatible when both sides have
watermarks and the predicate includes `event_time_between(...)`.

## Rejected Operations

These operation classes are rejected for the first slice:

```text
streaming.source_generation
streaming.sink_generation
streaming.trigger_policy
streaming.checkpoint_policy
streaming.streaming_order_by
streaming.streaming_limit
streaming.streaming_action
```

Selected-row helpers, ranking, lag/lead, rolling metrics, right/full/cross stream-stream joins, and arbitrary state APIs
remain batch-only until Structure defines a compiler-visible transformation contract for their state semantics. Source
generation, sink generation, triggers, checkpoints, and query lifecycle remain outside the transform compatibility
contract.

## Diagnostics

Diagnostics must include:

- transform name;
- operation kind;
- source location when available;
- streaming support classification: `compatible`, `batch_only`, or `unknown`;
- configured severity;
- the runtime-shape assumption, especially that joined side inputs are static;
- a user action;
- a link to `docs/background/Execution.back.md` or `docs/background/Execution.back.md`.

Required diagnostic cases:

- unknown hook without `streaming=True` in a transform that opts into streaming compatibility;
- stream-stream join missing required input modes, watermarks, or event-time bounds;
- stateful operation in a streaming-compatible transform;
- Spark action, RDD conversion, Pandas conversion, Python UDF, or local collection in a compiler-visible path;
- generated-source scan finding `readStream`, `writeStream`, `start()`, `awaitTermination()`, `collect()`, `count()`,
  `toPandas()`, `show()`, `.rdd`, or `mapInPandas` in a streaming-compatible generated transform body.

## Verification

Sprint 09 must add or schedule these checks:

- static compatibility tests for each supported and rejected operation family;
- direct runtime parity with a real streaming current input and static lookup side input;
- generated runtime parity for the same fixture;
- generated-source scans excluding lifecycle APIs and actions from compatible transforms;
- explain tests showing compatible, batch-only, and unknown classifications;
- documentation links from diagnostics to public reference pages.

If CI cannot run the live streaming fixture, a manual verification script must be checked in and named as release
blocking in Sprint 09.

## Acceptance Criteria

The first slice is complete when:

- a streaming-compatible transform can run online with a caller-supplied streaming DataFrame and return a streaming
  DataFrame plan;
- the generated class for the same transform returns an equivalent streaming DataFrame plan;
- stream-static left and inner lookup joins are accepted with static side inputs;
- the same fixture rejects a streaming side input or stream-stream join before runtime;
- batch-only analytical operations fail as errors when `streaming=True`;
- public docs explain both the supported first slice and the deferred features.

## Caller-Owned Migration Shapes

The caller-owned streaming contract also admits the following checked shapes. `session_window(event_time, gap)` is a
typed grouping key with a positive fixed interval. A streaming session aggregate requires an earlier watermark on the
same event-time field, at least one ordinary business key, and caller-applied `append` mode. Dynamic or invalid gaps,
global session groups, missing watermarks, and mismatched event-time fields are rejected before runtime.

`rowset_join(..., how="left"|"right"|"full")` is the bounded stream-stream outer-join form and `exists(...)` is the
semi-join form. Both streams must be declared `streaming=True`, both bound event-time fields must be watermarked, and
the predicate must include `event_time_between(...)`. The caller applies `append`; unmatched outer rows may wait for
watermark progress. Stream-static `exists(...)` keeps the streaming relation on the left, exposes no right fields, and
does not require a watermark. Static-left/stream-right, stream-static anti, right/full/cross forms, and `not_exists(...)`
are outside this slice.

The IR and shared recipe must carry join kind, input modes, watermark fields/delays, event-time bound, cardinality,
compatibility status, and required output mode. Online and generated paths consume identical recipes. Generated source
must not contain sources, sinks, checkpoint/trigger/output-mode calls, query lifecycle, actions, Pandas/RDD conversion,
or hidden UDF fallback. Chained stateful operations, dynamic session gaps, sorting/limits, analytic windows,
selected-row helpers, and arbitrary state remain rejected.

## Adoption Stages and Coverage Parity

Caller-owned adoption proceeds in three stages: static stream enrichment, left-outer static lookup, and exactly one
admitted stateful operation followed only by stateless work. Lookup projection requires a unique key or deterministic
dedupe policy; unmatched outer lookup fields are nullable. Each stage needs a test-owned file-stream fixture that stops
and restarts with the same checkpoint on PySpark 3.5 and 4.0.

Streaming coverage is measured against the checked transformation catalog. Every batch-supported family is classified
as `streaming-supported`, `streaming-partial`, `streaming-ineligible`, or `streaming-deferred`; partial families are
split into operation-level rows. V8 parity requires effective streaming coverage to be no lower than batch coverage,
with explicitly Spark-ineligible families removed from the denominator. Typed array-of-struct generators and exact
schema `union_all(...)`/`union_by_name(...)` are stateless candidates; distinct-style sets, arbitrary ordering/limits,
and priority selection remain ineligible unless a separate state contract proves otherwise.

## Streaming API Ledger

The checked streaming ledger classifies API families as `structure-supported`, `caller-owned-guided`, `design-gated`,
`streaming-ineligible`, or `out-of-scope`. It covers input-mode declarations, `isStreaming`, watermarks, stateful
transforms, stateless transforms, DataStreamReader sources, DataStreamWriter sinks/options, query lifecycle, side
effects, arbitrary state, RDD/Pandas boundaries, and Spark Connect streaming. Lifecycle APIs are never counted as
Structure transformation support.

`foreachBatch` is caller-owned-guided: the caller receives Structure's transformed DataFrame and applies the writer
chain, checkpoint, trigger, output mode, and lifecycle in caller code. `foreach` remains design-gated. Arbitrary state
APIs such as `applyInPandasWithState` and `transformWithState` require declared input/state/output Schemas, timeout and
clock policy, initialization and cleanup behavior, profile gating, generated-code rules, and restart evidence.

## Chained Event-Time Windows

The candidate `window_time(window_value)` accepts only a `TimeWindow` produced by the existing streaming `window(...)`
helper. The admitted candidate shape is one watermarked input, a first tumbling or sliding window aggregate, only
stateless projection/filtering between stages, and a second aggregate over `window_time(first_window)`. Generated code
uses public `pyspark.sql.functions.window_time`. Nested `window(window(...))`, a third stateful operation, session chains,
second-stage dedupe/join/session/selected-row operations, and missing watermarks are rejected with `STREAM-E0801` or
the registered state-composition diagnostic.

State-stage metadata records operation family, event-time source, watermark source, grouping keys, required output mode,
and whether another stateful stage is allowed. Explain reports the ordered stage list. A second stateful operation is
rejected unless it is the approved chained-window pair.

## Selected Rows and Analytic Windows

Window-scoped selected-row helpers are candidates only inside a watermarked event-time or session grouping window, with
deterministic scalar ordering and explicit tie policy. Global latest/earliest selection over an unbounded stream remains
a batch boundary. Broad `row_number`, `rank`, `dense_rank`, `lag`, `lead`, and rolling projections remain batch-only
unless a distinct finite-window API proves bounded state, frame semantics, and output mode. The existing batch helpers
must not silently change meaning for streaming callers.

## Side Effects and State

`foreach` and `foreachBatch` are not callable from Structure transform methods. A future Structure-owned side-effect API
would require sink identity, idempotence key, retry behavior, checkpoint and recovery policy, callback security review,
and live restart evidence. The generated transform module must remain free of `foreach`, `foreachBatch`, `writeStream`,
`start`, checkpoint, trigger, and sink calls.

## Final Acceptance

The consolidated streaming contract is complete when the streaming ledger and coverage denominator are checked, every
admitted operation has online/generated parity and PySpark 3.5/4.0 evidence, caller-owned lifecycle examples run, every
rejected shape fails before query start with a corrective diagnostic, and `make build` passes.
