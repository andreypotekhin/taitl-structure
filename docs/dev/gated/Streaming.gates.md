# Streaming Gates

This document is the single register for streaming design gates. Structure transforms return caller-supplied DataFrame
plans; they do not own sources, sinks, checkpoints, triggers, query start/stop, deployment, recovery, or side effects.
A gate identifies the state, lifecycle, diagnostic, generated-code, or live-evidence contract still needed before a
stronger Structure support claim.

## Status

- `design-gated`: a written admission direction exists, but Structure does not support the shape yet.
- `streaming-ineligible`: the shape requires batch materialization or is not safe for unbounded input.
- `caller-owned-guided`: Structure provides a runnable boundary recipe while the caller owns the streaming API.
- `unsupported`: the shape is outside the compiler-visible transformation contract.

The compiler should reject unsupported shapes before query start. Rejection is part of the contract, not an incomplete
implementation.

## Outstanding Gates

### Streaming Missing-Column Union — `design-gated`

Streaming schema evolution needs explicit cardinality, nullability, nested-field, alias, state, and PySpark 3.5/4.0
semantics and evidence. Exact-schema streaming unions remain supported; use them or materialize to batch until the
missing-column contract is proven.

### Broader Chained Stateful Operations — `design-gated`

The admitted chained-window shape is narrow: one watermarked event-time aggregate, stateless work, and one second
aggregate over `window_time(...)`. Broader chains need ordered state-stage metadata containing event-time and watermark
sources, grouping keys, retention, output mode, allowed following stages, diagnostics, generated form, and restart
evidence. Keep the one-stateful-plus-stateless policy for other chains.

### Row-Level `foreach` — `caller-owned-guided`

Structure supports an opt-in typed handoff for a declared final output. The caller constructs and attaches the writer,
starts the query, and owns sink identity, idempotence, retry, security, checkpoints, and recovery. Each attached stream
sink is an independent query. Classic PySpark 3.5 and 4.0 pass isolated callback and serialization checks; Spark Connect
is not claimed without equivalent proof. A failed writer query restarted from its checkpoint may replay rows and repeat
external effects; the local-file restart fixture passes online and generated on both claimed classic profiles. The
compile-time `foreach(row, sink)` marker is not rendered into generated modules, and `foreachBatch` remains caller-owned
through the adoption recipe.

### Arbitrary State Processors — `design-gated`

Positive support for row-based `transformWithState` is tracked by
[the row processor plan](../planning/P10042603.V11-transform-with-state.plan.md) for ordinary PySpark 4.1. PySpark 4.0's
Python API is `transformWithStateInPandas`, tracked separately by
[the Pandas processor plan](../planning/P10042604.V11-transform-with-state-in-pandas.plan.md) for ordinary PySpark 4.0
and 4.1. Both support claims remain gated on online/generated parity and restart evidence. `applyInPandasWithState`
remains outside these plans. `ArbitraryStateContract` validates adoption metadata only; it is not a state runtime.

The SearchDocuments proving lane and streaming-ineligible selected-row/window shapes are recorded in
[Streaming Deferred Work](../deferred/Streaming.deferred.md).

## Shared Admission Model

Every admitted stateful feature records its event-time source, watermark, grouping or partition key, state family,
caller-required output mode, allowed following state stage, generated public PySpark form, and corrective diagnostic.
Target evidence must cover the exact ordinary PySpark profiles each feature claims, online/generated parity, and
isolated file-stream restart behavior. A runtime unavailable for testing is unavailable evidence.

## Permanent Boundaries

Generated streaming sources and sinks, triggers, checkpoints, output modes, query names, start/stop, deployment,
recovery, `foreachBatch`, `foreach`, custom sinks, external side effects, and arbitrary state APIs remain caller-owned
or outside the Structure transform contract. See [Streaming deferred work](../deferred/Streaming.deferred.md).

## Related Records

- [API Catalog gates](ApiCatalog.gates.md) owns non-streaming and cross-family API gates.
- [Streaming deferred work](../deferred/Streaming.deferred.md) owns postponed lifecycle and orchestration direction.
- [Spark Streaming design](../design/SparkStreaming.design.md) owns the durable transformation boundary.
- [Streaming API reference](../../api/Streaming.api.md) is the user-facing support surface.
- [V10 streaming plan](../planning/P08022602.V10-streaming-state-and-join-contracts.plan.md) owns active
  implementation work.
- [V10 release evidence](../project-management/V10ReleaseEvidence.md) records unavailable proof lanes.
