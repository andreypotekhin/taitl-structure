# Spark Streaming Deferred Features

This specification records lifecycle and transformation features left outside the caller-owned streaming compatibility
slice. Source, sink, and lifecycle policy remain caller-owned; transformation features remain unsupported until they
receive compiler-visible state semantics, diagnostics, tests, and public documentation. The Sprint 18 exceptions are
specified in [SparkStreaming.md](SparkStreaming.md).

## Caller-Owned Lifecycle Features

The first compatibility slice does not generate or manage:

- `spark.readStream`;
- `df.writeStream`;
- output sinks;
- triggers;
- checkpoint locations;
- query names;
- `start()`, `awaitTermination()`, stop behavior, deployment, or recovery.

These are caller-owned operational concerns for all transform classes.

## Deferred Capability Names

The backend capability model must reserve these lifecycle names so the first slice can reject them and v3 can admit
them deliberately:

```text
streaming.source_generation
streaming.sink_generation
streaming.trigger_policy
streaming.checkpoint_policy
streaming.output_mode
streaming.query_lifecycle
streaming.streaming_selected_row
streaming.streaming_ranking_window
streaming.streaming_lag_lead
streaming.streaming_rolling_window
streaming.foreach_batch
streaming.foreach_sink
```

When a transform, hook, or generated artifact requires an unsupported capability, the compiler must fail through a
backend capability or streaming compatibility diagnostic. It must not silently fall back to a hook, SQL string, local
collection, RDD conversion, or Python UDF.

## Watermarks And State Policy

Watermarks are now transform-scoped DataFrame operations through `watermark(...)`. Future state policies must define:

- event-time field declaration;
- allowed delay thresholds;
- late-data behavior;
- state retention expectations;
- output mode compatibility;
- diagnostics when a stateful operation lacks a required watermark or state policy;
- explain output showing state assumptions.

Policy must be source and operation aware. A transform-local default is not enough for complex multi-state plans.

## Stateful Operations

Stateful streaming support is admitted by operation family:

- grouped aggregations with watermarks;
- exact and subset dedupe with watermarks;
- inner stream-stream joins with watermarks and event-time bounds;
- windowed aggregations;
- selected-row helpers such as latest/earliest;
- ranking, lag/lead, and rolling windows where Spark admits them;
- outer and semi stream-stream joins, scheduled by the v4 migration specification;
- session-window aggregation with a static gap, scheduled by the v4 migration specification.

Each family must define accepted output modes, required watermarks, state growth risks, deterministic tie policy, and
live streaming tests. Until then, the operation remains `batch_only` for streaming compatibility.

V8 closes two previously ambiguous families as explicitly ineligible rather than deferred:

- arbitrary relation ordering and bounds: `order_by(...)`, `limit(...)`, and `offset(...)`;
- row-number style priority selection: `select_first_qualified(...)`.

These require a batch materialization boundary for v8 caller-owned streaming. They are not hidden implementation gaps
in the v8 coverage denominator.

## Hooks And Foreach

`streaming=True` only admits ordinary DataFrame-returning hooks inside the first slice. V9 provides caller-owned
guidance for `foreachBatch` through `examples.streams.adoption.start_foreach_batch_query(...)`, which callers invoke
after Structure returns a transformed streaming DataFrame. Generated transform modules still must not contain
`foreachBatch`, `writeStream`, query lifecycle calls, checkpoints, or triggers.

The adoption helper requires a `ForeachBatchSafety` declaration for stable sink identity, idempotence key, retry policy,
and serving snapshot identity. It validates that metadata before query start but cannot enforce callback idempotence or
transactionality; those remain caller responsibilities.

Row-level `foreach`, external side effects inside transforms, and custom sink hooks still need a separate lifecycle and
idempotence contract. They must make retry behavior, side effects, and failure handling explicit before Structure can
own them.

## Acceptance For Future Admission

A deferred feature can move into a sprint only when its plan includes:

- design, implementation specification, and public reference updates;
- backend capability support and unsupported cases;
- diagnostics with links and concrete user actions;
- explain output;
- generated-source snapshots;
- execution/generated-code parity where relevant;
- live Spark Structured Streaming evidence;
- troubleshooting entries for likely operational failures.

## V9 Reclassification

V9 reclassifies these deferred features in a checked PySpark streaming API ledger before implementing any new support
claim. Lifecycle APIs may become `caller-owned-guided` through runnable examples and diagnostics while still remaining
outside generated transform modules. A lifecycle API can become Structure-owned only after a separate product decision
records operational semantics, checkpoint/recovery behavior, side-effect idempotence, security review, and live
evidence.

The v9 design-gated streaming rows now have dedicated design and implementation specifications:
[SparkStreaming.md](../design/SparkStreaming.md) and
[SparkStreaming.md](SparkStreaming.md). The broader active execution plan is
`docs/dev/planning/P07302601.V9-api-catalog-design-gates.plan.md`.
