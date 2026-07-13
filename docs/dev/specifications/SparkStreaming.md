# Spark Streaming Transformation Support

This specification defines Spark Structured Streaming support for Structure transforms. Structure compiles DataFrame
transformations that may run on streaming DataFrames. In v1/v2 the caller owns streaming lifecycle code; v3 adds a
separate Structure-owned lifecycle orchestration contract.

## Support Claim

Structure supports a transform with a streaming input when all of these are true:

- the configured backend is PySpark;
- the caller creates the streaming source and passes the streaming DataFrame into online or generated execution;
- the transform has one current pipeline input that may be streaming;
- joined side inputs are static DataFrames;
- every compiler-visible operation is classified as streaming-compatible;
- opaque hooks are absent or explicitly marked `streaming_safe=True`;
- generated and online execution do not emit or call streaming lifecycle APIs, Spark actions, RDD conversion, Pandas
  conversion, Python UDFs, or local collection.

The v1/v2 support claim covers returned DataFrame plans only. Structure-owned streaming sources, sinks, query
start/stop behavior, triggers, checkpoints, query names, deployment, and recovery belong to the v3 orchestration
contract.

## Configuration

The existing configuration key remains:

```toml
[tool.structure]
streaming_compatibility_checks = true
```

Transform-level opt-in remains:

```python
@transform(streaming_compatible=True)
class EnrichOrders(Transform):
    ...
```

Severity rules:

- if checks are disabled and no transform opts in, no streaming diagnostics are emitted;
- if checks are enabled and the transform does not opt in, incompatible operations are warnings;
- if the transform opts in, incompatible and unknown operations are errors, even when global checks are disabled.

## Runtime Contract

Online execution uses the existing session:

```python
session = StructureSession(spark=spark, ctx=ctx, config=config)
result = EnrichOrders(orders=orders_stream, customers=customers_static).run(session)
```

Generated execution uses the existing generated class API:

```python
result = EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
    orders=orders_stream,
    customers=customers_static,
)
```

For a compatible transform, `result.isStreaming` should be true when the current input is streaming. Structure must not
branch on `isStreaming` in generated transform bodies. The same DataFrame plan should be valid for batch and streaming
inputs when the operation contract is satisfied.

## Supported Operations

The first slice supports these operation classes:

```text
streaming.row_local_projection
streaming.row_local_filter
streaming.schema_only_validation
streaming.watermark
streaming.stream_static_left_join
streaming.stream_static_inner_join
streaming.streaming_safe_hook_boundary
```

Projection and filtering are compatible only when every expression lowers to Spark Column operations that do not need
cross-row state, actions, UDFs, RDD conversion, Pandas conversion, or local collection.

Schema-only validation may inspect DataFrame schema metadata, column names, Spark data types, and nullability metadata.
It must not trigger a Spark job.

Stream-static joins are compatible only for left and inner joins where the current pipeline side may be streaming and
the joined side is static. Static-side broadcast hints are allowed when already supported by the PySpark target.

Hooks are compatible only when the hook is marked `streaming_safe=True`. The checker treats this as a trusted boundary,
not as proof from body analysis.

`input(..., streaming=StreamingMode.YES | NO)` records whether a transform input is explicitly streaming. The default
`NO` treats joined side inputs as static unless the author declares them streaming.

`watermark(field, delay="10 minutes")` records a compiler-visible event-time watermark and lowers to PySpark
`DataFrame.withWatermark(...)`. Watermarks are transformation metadata, not lifecycle ownership.
Online execution interprets that lowered recipe directly, and generated execution renders the same call. A watermark on
a lookup relation is applied before that relation is joined.

`event_time_between(left_time, right_time, upper=..., lower="0 seconds")` records the bounded event-time relationship
required for supported stream-stream joins and lowers to a Spark-visible boolean predicate.

Grouped aggregations and exact/subset dedupe are streaming-compatible when a watermark appears earlier in the same
step method on the current streaming frame. Explain output reports the Spark output modes the caller must use, but
Structure never calls `writeStream.outputMode(...)`.

An inner `rowset_join(...)` between two inputs declared `StreamingMode.YES` is compatible when both sides have
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
generation, sink generation, triggers, checkpoints, and query lifecycle remain outside the v1/v2 transform
compatibility contract and belong to v3 streaming orchestration.

## Diagnostics

Diagnostics must include:

- transform name;
- operation kind;
- source location when available;
- streaming support classification: `compatible`, `batch_only`, or `unknown`;
- configured severity;
- the runtime-shape assumption, especially that joined side inputs are static;
- a user action;
- a link to `docs/reference/SparkStreaming.md` or `docs/reference/SparkStreamingDeferredFeatures.md`.

Required diagnostic cases:

- unknown hook without `streaming_safe=True` in a transform that opts into streaming compatibility;
- stream-stream join missing required input modes, watermarks, or event-time bounds;
- stateful operation in a streaming-compatible transform;
- Spark action, RDD conversion, Pandas conversion, Python UDF, or local collection in a compiler-visible path;
- generated-source scan finding `readStream`, `writeStream`, `start()`, `awaitTermination()`, `collect()`, `count()`,
  `toPandas()`, `show()`, `.rdd`, or `mapInPandas` in a streaming-compatible generated transform body.

## Verification

Sprint 09 must add or schedule these checks:

- static compatibility tests for each supported and rejected operation family;
- online runtime parity with a real streaming current input and static lookup side input;
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
- batch-only analytical operations fail as errors when `streaming_compatible=True`;
- public docs explain both the supported first slice and the deferred features.
