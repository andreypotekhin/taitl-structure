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

Two exact pairs are admitted: a watermarked event-time window aggregate followed by a second aggregate over
`window_time(...)`, and, on ordinary PySpark 3.5.0 and 4.0.0, watermarked deduplication followed by one watermarked
event-time window aggregate in Append mode. For the dedupe-to-window pair, per-operator `budget(...)` declarations,
`state_budget_checking`, and a caller-attached progress guard are implemented. Live tests cover online/generated
parity, Append output, same-checkpoint restart, and operator-progress mapping on both profiles.

The broad gate remains for every other composition: reversed order, joins, state processors, and a third stateful
operation. The PySpark 4.1 row `transformWithState` support and the Pandas processor implementation do not admit either
processor into a chain. Broader admission needs an explicit stage-transition contract for watermark propagation,
per-stage retention and output compatibility, restart/checkpoint behavior, rejection diagnostics, generated lowering,
and profile-specific live evidence. The narrow pair's current live fixture does not separately assert behavior for an
event arriving beyond the watermark or state eviction; add that coverage before treating those details as demonstrated.

### Row-Level `foreach` — `caller-owned-guided`

Structure supports an opt-in typed handoff for a declared final output. The caller constructs and attaches the writer,
starts the query, and owns sink identity, idempotence, retry, security, checkpoints, and recovery. Each attached stream
sink is an independent query. Classic PySpark 3.5, 4.0, and 4.1 pass isolated callback and serialization checks; Spark Connect
is not claimed without equivalent proof. A failed writer query restarted from its checkpoint may replay rows and repeat
external effects; the local-file restart fixture passes online and generated on both claimed classic profiles. The
compile-time `foreach(row, sink)` marker is not rendered into generated modules, and `foreachBatch` remains caller-owned
through the adoption recipe.

### Row `transformWithState` — `structure-supported`

The typed and native row interfaces are supported on ordinary PySpark 4.1 for Append/Update output modes and None,
ProcessingTime, and EventTime modes in their valid combinations. Online/generated parity covers composite keys,
processing-time and event-time timers, and same-checkpoint restart. Complete fails during compilation. The typed path
declares named Value/List/Map state attributes, supports typed initial state, and accepts TTL only with ProcessingTime.
`external_state_processor(...)` keeps unsupported Python constructs and additional PySpark features available as an
opaque boundary. Persisted state changes are checkpoint-sensitive and Structure does not migrate state. PySpark 4.0
does not expose the row processor entry point. See the
[row admission plan](../planning/past/P10062603.V11-transform-with-state-admission-and-typed-parity.plan.md),
[typed parity design](../design/V11TransformWithStateTypedParity.design.md), and
[row processor plan](../planning/past/P10042603.V11-transform-with-state.plan.md).

### Legacy `applyInPandasWithState` — `design-gated`

The separate `apply_in_pandas_with_state(...)` compiler surface targets ordinary PySpark 3.5, 4.0, and 4.1 through
`GroupedData.applyInPandasWithState`; accumulation and same-checkpoint restart evidence passes on the three pinned
ordinary profiles, while timeout and zero/multiple-output coverage remains pending. See the
[legacy Pandas state plan](../planning/P10062602.V11-apply-in-pandas-with-state.plan.md).

### Dataset State APIs — `unsupported`

`mapGroupsWithState` and `flatMapGroupsWithState` are JVM Dataset APIs without PySpark entry points. The
`ArbitraryStateContract` validates adoption metadata only; it is not a state runtime.

The SearchDocuments proving lane and streaming-ineligible selected-row/window shapes are recorded in
[Streaming Deferred Work](../deferred/Streaming.deferred.md).

## Shared Admission Model

Every admitted stateful feature records its event-time source, watermark, grouping or partition key, state family,
caller-required output mode, allowed following state stage, generated public PySpark form, and corrective diagnostic.
Target evidence must cover the exact ordinary PySpark profiles each feature claims, online/generated parity, and
isolated file-stream restart behavior. A runtime unavailable for testing is unavailable evidence.

## Permanent Boundaries

Generated streaming sources and sinks, triggers, checkpoints, output modes, query names, start/stop, deployment,
recovery, `foreachBatch`, `foreach`, custom sinks, external side effects, Pandas state gates, and Dataset-only state
APIs remain caller-owned or outside the Structure transform contract. See
[Streaming deferred work](../deferred/Streaming.deferred.md).

## Related Records

- [API Catalog gates](ApiCatalog.gates.md) owns non-streaming and cross-family API gates.
- [Streaming deferred work](../deferred/Streaming.deferred.md) owns postponed lifecycle and orchestration direction.
- [Spark Streaming design](../design/SparkStreaming.design.md) owns the durable transformation boundary.
- [Streaming API reference](../../api/Streaming.api.md) is the user-facing support surface.
- [V10 streaming plan](../planning/past/P08022602.V10-streaming-state-and-join-contracts.plan.md) owns active
  implementation work.
- [V10 release evidence](../project-management/V10ReleaseEvidence.md) records unavailable proof lanes.
