# Spark Streaming Deferred Features Design

The first Spark streaming slice supports caller-owned streaming DataFrames only. This document designs the features
left out of that slice so they remain visible, intentional, and schedulable instead of becoming ad hoc extensions to
the PySpark target.

## Deferred Families

### Source Ownership

Generated streaming source ownership means Structure would emit `spark.readStream` code. That requires source
declarations for table, path, format, options, schema handling, fail-on-missing behavior, and environment-specific
credentials. The first slice leaves all of this to caller code because source configuration is operational policy, not
transform logic.

### Sink Ownership

Generated sink ownership means Structure would emit `writeStream` code and possibly table creation or storage layout
policy. That requires sink declarations for format, table or path target, partitioning, output mode compatibility,
checkpoint location, trigger, error handling, and query naming. It is deferred because generated transform classes
currently return DataFrames rather than owning writes.

### Query Lifecycle

Triggers, checkpoints, query names, `start()`, `awaitTermination()`, stop behavior, and restart behavior form the
streaming lifecycle. Structure must not partially own this lifecycle. A later design should introduce one coherent
job model with idempotent deployment, recovery, and diagnostics for missing checkpoint configuration.

### Watermarks And State Policy

Watermarks and state policies define how long Spark keeps streaming state and when late data is dropped. They are
required for many stream-stream joins, windowed aggregations, and stateful dedupe forms. Structure cannot infer safe
values from transform code alone; users need explicit event-time fields, delay thresholds, tie policies, and output
mode expectations.

### Stateful Operations

Aggregations, windowed aggregations, selected-row helpers, ranking, lag/lead, rolling metrics, exact/subset dedupe, and
many full rowset joins may be legal in Spark streaming only with specific state and output-mode contracts. The first
slice classifies them as batch-only for streaming support. A later slice should admit them one family at a time with
capabilities, diagnostics, explain output, and live streaming tests.

### Stream-Stream Joins

Stream-stream joins need declared input modes, watermark relationships, event-time constraints, join type limits, and
state retention policy. The first slice admits only stream-static left and inner lookup joins because they do not
require Structure to synchronize two streaming inputs.

### Arbitrary Hooks And Foreach Logic

Opaque hooks remain user-owned. Certifying arbitrary hook bodies, `foreachBatch`, `foreach`, external side effects,
and custom sink code would require a different safety model. A later design may add target-scoped lifecycle hooks, but
the first slice should keep hook participation limited to an explicit `streaming_safe=True` promise.

## Future Design Shape

Future streaming orchestration should be explicit and typed:

```python
@streaming_job(
    source=stream.table("orders").schema(OrderRaw),
    sink=stream.table("orders_enriched").checkpoint("${CHECKPOINT_ROOT}/orders_enriched"),
    trigger=Trigger.processing_time("1 minute"),
    output_mode=OutputMode.APPEND,
)
class EnrichOrdersJob(StreamJob):
    transform = EnrichOrders
```

This example is illustrative, not a committed API. The important design rule is that lifecycle policy should be
declared outside ordinary transform methods. Transforms describe how rows change. Jobs describe where streaming rows
come from, where they go, and how Spark manages query state.

## Admission Rules

A deferred streaming feature becomes eligible only when it has:

- a public DSL shape or configuration key;
- backend capability names and unsupported decisions;
- diagnostics with concrete user actions;
- explain output showing lifecycle and state assumptions;
- online and generated parity where both modes apply;
- live Spark Structured Streaming evidence;
- public reference documentation.

No deferred feature should be admitted through hidden SQL strings, Python UDF fallbacks, local collection, RDD
conversion, or a hook-only path.

## Scheduling

Sprint 09 owns the first slice and this deferred-feature reference. v3 remains the natural home for full streaming
orchestration. Individual stateful operation families may be pulled forward only if they preserve the first-slice
principle: explicit lifecycle policy, fail-early diagnostics, and live streaming evidence before support is claimed.
