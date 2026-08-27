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

### Row-Level `foreach` — `design-gated`

Row callbacks require sink identity, idempotence, retry, security, checkpoint, and recovery contracts. Keep `foreach`
outside transforms and generated modules. `foreachBatch` is `caller-owned-guided` through the adoption recipe and does
not create a Structure-owned sink runtime.

### Arbitrary State Processors — `design-gated`

`applyInPandasWithState` and `transformWithState` need typed input, state, and output schemas; timeout and clock policy;
initialization and cleanup; target profiles; generated boundaries; and restart evidence. `ArbitraryStateContract`
validates adoption metadata only; callers own the state runtime.

The SearchDocuments proving lane and streaming-ineligible selected-row/window shapes are recorded in
[Streaming Deferred Work](../deferred/Streaming.deferred.md).

## Shared Admission Model

Every admitted stateful feature records its event-time source, watermark, grouping or partition key, state family,
caller-required output mode, allowed following state stage, generated public PySpark form, and corrective diagnostic.
Target evidence must cover PySpark 3.5 and 4.0 online/generated parity and isolated file-stream restart behavior where
the feature claims runtime support.

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
