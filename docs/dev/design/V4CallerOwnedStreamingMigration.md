# V4 Caller-Owned Streaming Migration

## Purpose

V4 makes moving a transformation from a batch PySpark DataFrame to a Structured Streaming DataFrame predictable, and makes existing plain-PySpark streaming transformations easier to express in Structure. The user keeps the code that selects a source, writes a sink, configures a checkpoint and trigger, and starts the query. Structure owns only the typed, compiler-visible DataFrame transformation between those caller-owned boundaries.

The first migration slice admits only common stateful shapes whose state and late-data rules can be seen in the transform plan: session-window aggregation, bounded stream-stream outer joins, stream-stream semi joins, and stream-static semi filtering. It is a transformation program, not a streaming-job framework.

## User Model

A user may keep their existing query shell and replace the central DataFrame transformation with a Structure transform:

    orders = spark.readStream.table("orders")
    payments = spark.readStream.table("payments")
    result = CorrelateOrders(orders=orders, payments=payments).run(session)
    query = result.writeStream.option("checkpointLocation", checkpoint).outputMode("append").start(target)

`readStream`, `writeStream`, checkpoint selection, trigger selection, output-mode application, query naming, deployment, start/stop, recovery, `foreach`, and `foreachBatch` remain outside Structure. Explain output tells the user when an admitted transformation requires `append`; it never applies that mode itself.

## Admitted V4 Shapes

### Session-Window Aggregation

`session_window(event_time, gap)` is a typed grouping-key expression. It has the existing `TimeWindow` shape: non-null `start` and `end` timestamps. The first version accepts only a static, positive Spark interval `gap`; dynamic per-row gap expressions are deferred.

For a streaming input, the compiler admits a session aggregate only when a watermark on the same event-time field precedes it in the step, the grouping includes at least one ordinary business key in addition to the session window, and explain reports `StreamingOutputMode.APPEND`. The user receives a diagnostic for a missing watermark, a global session group, a non-static or non-positive gap, or an incompatible requested output mode. Session merge tuning remains Spark caller configuration and is not a Structure API.

### Bounded Stream-Stream Joins

The existing `rowset_join(...)` API remains the outer-join spelling. `"left"`, `"right"`, and `"full"` are admitted when both named inputs declare `streaming=True`, the required watermark is present on each input, and the predicate includes `event_time_between(...)`. `exists(...)` remains the semi-join spelling and is admitted for the same bounded stream-stream shape.

Every admitted stream-stream outer or semi join requires `append`. Explain and diagnostics must say that unmatched outer rows can be emitted only after watermark progress establishes that a future match is no longer possible; a quiet input can therefore delay output. The plan records the join kind, both input modes, each watermark, and the time bound so the checker, online runner, and generated renderer consume the same contract.

### Stream-Static Semi Filtering

`where(exists(on=...))` is admitted when the current/left input is streaming and the consulted right input is static. It is a non-stateful existence filter and needs no watermark. It must preserve the current row shape, expose no right-side fields, and lower to a left-semi join or an equivalent Spark-plan-visible form. `not_exists(...)` and all directions that place the streaming relation on the right remain unsupported in v4.

## Compiler Contract

The implementation adds distinct backend capabilities for session aggregation, stream-static semi filtering, stream-stream outer joins, and stream-stream semi joins. The intermediate representation records the operation's streaming capability, required output modes, watermarks, input modes, and event-time-bound facts. The shared PySpark recipe preserves those facts for both online execution and generated PySpark.

The streaming compatibility pass classifies a transform as compatible only when every relevant operation satisfies its shape. It must fail early with a precise remedy rather than relying on Spark to reject a partially specified query. Required diagnostic cases are missing declared streaming input mode, missing required watermark, missing time bound, invalid session grouping key, invalid session gap, and a forbidden output mode. `structure explain` must show the stateful operation, its watermarks, bound, and caller-required mode.

## Boundaries

The following remain outside v4: chained stateful operations, chained time-window aggregation, global or otherwise unbounded aggregation and deduplication, sorting and limits, analytic windows and selected-row helpers, dynamic session gaps, arbitrary state processors, Pandas and RDD boundaries, streaming sources/sinks, lifecycle APIs, and stream-static right/full/cross/anti shapes. These exclusions keep Structure from owning operational policy or claiming safe state behavior it cannot model.

## Evidence Standard

No planned form becomes implemented without symbolic/IR tests, capability tests, rendered-source tests, online/generated parity, explain assertions, public documentation, and live Structured Streaming evidence for both the PySpark 3.5.x and 4.0.x target lines. The live test owns and cleans up its own temporary source, sink, checkpoint, and query; that test-harness ownership does not change the public lifecycle boundary.

