# Streaming

Structure supports compiler-visible Spark Structured Streaming transformations while leaving streaming lifecycle with
the caller. A caller creates streaming and static DataFrames, passes them to an ordinary Structure transform, and owns
sources, sinks, checkpoints, triggers, output modes, query start/stop, deployment, and recovery.

The [Streaming API](../api/Streaming.api.md) lists supported declarations and parity. This background combines runtime
shape, compatibility analysis, supported operations, deferred features, hooks, validation, and generated-code rules.
The normative sources are [Spark Streaming](../dev/specifications/SparkStreaming.md),
[Streaming Compatibility](../dev/specifications/StreamingCompatibility.md), and the
[Spark Streaming design](../dev/design/SparkStreaming.md).

## Runtime Ownership Boundary

The supported shape is one streaming current pipeline input plus optional static side inputs:

```python
orders = spark.readStream.table("orders")
customers = spark.read.table("customers")

result = EnrichOrdersGenerated(spark=spark).run(
    orders=orders,
    customers=customers,
)

query = result.writeStream.option("checkpointLocation", checkpoint).toTable("orders_enriched")
```

Structure owns the checked transformation plan and returns a DataFrame plan. It does not generate `readStream` or
`writeStream`, start or stop queries, set checkpoints or triggers, apply output modes, or perform external side effects.

Streaming compatibility means the generated or online transformation can accept the concrete streaming DataFrame shape,
and every operation on streaming data is admitted by the compiler-visible policy. It does not mean Structure starts a
streaming query.


## Declaring Compatibility

Transform-level opt-in is an explicit all-step contract:

```python
@transform(streaming=True)
class EnrichOrders(Transform):
    enriched = output(OrderEnriched)
    ...
```

The configuration seed is:

```toml
streaming_compatibility_checks = true
```

With checks enabled, a non-streaming transform receives warnings for incompatible or unknown shapes, while
`streaming=True` turns incompatible and unknown shapes into errors. If checks are disabled, an explicit transform marker
still runs compatibility analysis for that transform. Streaming inputs and composed streaming outputs trigger analysis
without implicitly changing transform options.

The result classification is:

```text
compatible  every operation is admitted
batch_only  at least one operation is known to be incompatible
unknown     an opaque operation prevents proof, with no known incompatibility
```

An undeclared stream-to-batch boundary follows `stream_to_batch_policy`. A strict policy requires an explicit streaming
declaration or `allow_stream_to_batch=True`; no allowance suppresses a known incompatible operation.


## Supported Stateless Operations

Projection is compatible when every value is a compiler-visible row-local Spark `Column` expression. Filtering is
compatible when every predicate is a compiler-visible row-local boolean expression. Expression helpers and scalar
`@special(type="udf")` calls are compatible when they do not require cross-row state, local collection, or RDD
conversion.

Schema-only validation is compatible. It may inspect column names, data types, nullability metadata, and nested shape,
but may not trigger Spark jobs. Compiler traceability and generated metadata are compatible when they record
compile-time information only.

Watermarks are compatible when declared with `watermark(field, delay=...)` before the stateful operation they support.
The same transform semantics must apply in online and generated-code execution.


## Stateful Operations And Joins

Batch grouped aggregations remain supported. Streaming business-key aggregations may require caller-owned `update` or
`complete` output modes and can retain unbounded state; Structure reports this advisory risk as `STREAM-W0802`.
Event-time and session-window aggregations require a compiler-visible watermark on the grouped event-time field.
Watermarked dedupe is admitted only in the documented bounded shapes.

Stream-static joins are allowed when the current pipeline is streaming and the side input is static. `exists(...)` may
provide stream-static left-semi filtering. Broadcast hints apply only to the static side.

Bounded stream-stream joins require both inputs to declare `streaming=True`, watermarks on both sides, and an event-time
bound such as `event_time_between(left_time, right_time, upper=...)`. Structure admits only shapes whose state and
retention policy are compiler-visible.


## API Ledger And Stateful Boundaries

The streaming API ledger classifies each compiler-visible operation as compatible, batch-only, rejected, or unknown for
the concrete input lineage. The classification records operation family, current versus side-input lineage, required
watermark or event-time bound, state retention assumptions, hook declaration, and diagnostic severity. An opaque hook or
unclassified operation cannot silently become compatible.

Chained event-time windows require each stateful stage to declare its event-time input, watermark relationship, and
retention boundary. A later window cannot claim the watermark of an unrelated or already-invalidated field. Selected-row
and analytic-window helpers such as `latest_by(...)`, ranking, lag/lead, rolling windows, and unbounded dedupe remain
batch-only unless a dedicated bounded-state contract admits the exact shape.

Finite grouped `first_value(...)` and `last_value(...)` may be used inside a watermarked event-time aggregate window;
that is not a general streaming reinterpretation of selected-row or analytic-window helpers. Streaming state remains a
target policy, not a hidden Structure-owned store.

The implementation must keep streaming-specific state, output-mode, and retention decisions in capability and
compatibility metadata. It must not create sources, sinks, triggers, checkpoints, query lifecycle, or recovery logic.

## Hooks And Validation

Hooks are opaque because Structure cannot safely inspect arbitrary PySpark code. Mark a hook `streaming=True` only when
it returns a DataFrame, avoids actions, RDD/Pandas conversion, lifecycle APIs, external side effects, and unmodeled
state:

```python
@raw(inout=lane(orders) | lane(orders), streaming=True)
def keep_valid(self, *, orders, spark, ctx):
    return orders.where(F.col("id").isNotNull())
```

Structure trusts the marker and records it in traceability; it does not prove the hook body safe.

Input, intermediate, and output validation is streaming-compatible only when it is schema-only. Row-level constraints,
uniqueness checks, sampling, counts, collections, and other scans classify the plan as batch-only unless a future policy
proves them safe.


## Compile-Time And IR Contract

The checker runs after symbolic execution and before code generation, without starting Spark. Each IR operation exposes
its support classification and enough metadata to explain the decision:

- operation kind and source transform/step;
- source location or expression text when available;
- referenced input scopes and current versus side-input lineage;
- join type, hint, watermark, and event-time bound;
- validation mode and hook streaming declaration.

The checker must reject or warn on unsupported operations, incompatible stream-stream joins, unsafe hooks, non-schema
validation, and unknown opaque boundaries. It preserves the classification in compile reports and traceability.


## Generated-Code Requirements

Generated PySpark must use DataFrame and Column operations for compiled DSL operations, avoid actions and RDD/Pandas
conversion, keep schema-only validation action-free, and call streaming-safe hooks without lifecycle wrapping. It may
use
the same generated class for batch and streaming DataFrames because the caller supplies the runtime shape.


## Deferred And Rejected Operations

The following remain batch-only or deferred for streaming inputs:

- global sort, limit, offset, global top-N, and unbounded distinct/deduplication;
- event-time aggregations without a matching preceding watermark;
- chained windowed or stateful aggregations beyond the admitted single-stage shapes;
- ranking, lag/lead, rolling windows, and selected-row helpers such as `latest_by(...)`;
- cross, anti, or unbounded stream-stream joins;
- Pandas UDFs, RDD operations, `mapInPandas`, `foreachPartition`, and local Spark actions;
- arbitrary hooks without an explicit streaming-safe declaration;
- generated lifecycle, custom sinks, `foreachBatch`, `foreach`, and arbitrary state APIs.

Finite grouped `first_value(...)` and `last_value(...)` remain possible inside a watermarked event-time window. They are
aggregate expressions, not a streaming reinterpretation of batch selected-row or analytic-window helpers.


## Diagnostics

Streaming diagnostics should identify the transform, step, concrete input lineage, operation, state assumption, and
shortest fix. Typical fixes are to remove a batch-only operation, make a side input static, add a watermark and
event-time
bound, explicitly mark a safe hook, or keep lifecycle code in caller-owned Spark code.

```text
CompileError STREAM-E0801: Transform is not streaming-compatible

Problem:
  stream-stream joins require declared streaming inputs, watermarks on both event-time fields, and an event-time bound.

Use:
  declare both streaming inputs, add watermarks and event_time_between(...), or keep the transform batch-only.

See docs/background/Streaming.back.md
```


## Appendix: Deferred Feature Admission

A deferred streaming feature may be admitted only when Structure defines its public DSL or configuration, capability
requirements, diagnostics, explain output, online/generated parity, live Spark verification, and troubleshooting
guidance.
V10 may add state-stage lists, more bounded stream-stream joins, and explicit retention rules, but sources, sinks,
triggers, checkpoints, output modes, deployment, recovery, and external side effects remain caller-owned.

