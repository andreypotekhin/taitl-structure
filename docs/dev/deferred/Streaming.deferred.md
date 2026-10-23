# Streaming Deferred Work

This document records streaming lifecycle and orchestration work intentionally deferred beyond the current
caller-owned transformation boundary. Current admission gates are in [Streaming Gates](../gated/Streaming.gates.md).

## Deferred Families

### Source Ownership

Generated streaming source ownership means Structure would emit `spark.readStream` code. That requires source
declarations for table, path, format, options, schema handling, fail-on-missing behavior, and environment-specific
credentials.

### Sink Ownership

Generated sink ownership means Structure would emit `writeStream` code and possibly table creation or storage layout
policy. That requires sink declarations for format, table or path target, partitioning, output mode compatibility,
checkpoint location, trigger, error handling, and query naming.

### Query Lifecycle

Triggers, checkpoints, query names, `start()`, `awaitTermination()`, stop behavior, and restart behavior form the
streaming lifecycle. A later design should introduce one coherent job model with idempotent deployment, recovery, and
diagnostics for missing checkpoint configuration.

### Watermarks And State Policy

Watermarks, finite state-stage metadata, per-operator budgets, and progress guidance are implemented for the admitted
shapes. Broader state policies still need explicit event-time fields, delay thresholds, retention, tie behavior,
output compatibility, and restart evidence.

### Stateful Operations

Watermarked event-time/session aggregation, dedupe, supported stream joins, two finite stateful pairs, and the
admitted row/Pandas processors are implemented. Global selected-row helpers, ranking, lag/lead, rolling projections,
and additional compositions retain their gates. Consult the [Streaming reference](../../reference/Streaming.ref.md)
for each operation's mode, state, and evidence limits.

### Stream-Stream Joins

Admitted stream-stream joins have declared input modes, watermarks, event-time bounds, and join-type limits. Cross,
anti, and other unsupported shapes need their own state-retention and output-mode evidence.

### Arbitrary Hooks And Foreach Logic

Typed `foreach` and `foreach_batch` handoffs and safety declarations are implemented. Callers construct and attach
writers, own retries and idempotence, and start queries. Certifying arbitrary hook bodies or managing external effects
would require a separate operational model.

## Future Design Shape

Future streaming orchestration should be explicit and typed. Transforms describe how rows change. Jobs describe where
streaming rows come from, where they go, and how Spark manages query state.

## Admission Rules

A deferred streaming feature becomes eligible only when it has a public DSL shape or configuration key, backend
capability names, diagnostics, explain output, execution and generated-code parity where relevant, live Spark
Structured Streaming evidence, and public reference documentation.

## Deferred Scope

- stateful operation families with capability checks, explain output, and live evidence;
- additional stream-stream join shapes with declared state and retention semantics; and
- a safety and idempotence model for arbitrary hooks, `foreachBatch`, `foreach`, external side effects, and custom
  sinks.

## Permanent Non-Goals

Structure does not own `readStream`, `writeStream`, triggers, checkpoints, output-mode application, query names,
query start/stop, deployment, recovery, `foreachBatch`, `foreach`, custom side-effect sinks, external side effects, or
general state APIs outside the admitted processor interfaces.

## Transformation Features

More complex stateful transformation features remain deferred until their state semantics are compiler-visible:

- chained stateful operations outside the two documented finite pairs;
- cross and anti stream joins;
- selected-row helpers such as latest or earliest on streaming inputs;
- ranking, lag/lead, and rolling windows; and
- general state APIs outside the admitted processor interfaces.

### Streaming-Ineligible Selection and Windows

Global `latest_by(...)`, `earliest_by(...)`, and dedupe variants do not have finite state without a watermark,
event-time bound, timeout, or window. Broad `row_number(...)`, `rank(...)`, `dense_rank(...)`, `lag(...)`, `lead(...)`,
and rolling projection windows remain batch-only. Grouped `first_value(...)` and `last_value(...)` within a watermarked event-time window provide the admitted finite
selection alternative; broader projection windows remain excluded.

### SearchDocuments Streaming Proving Lane

The SearchDocuments batch graph still needs bounded ranking state, finite stream/stream joins, append-final output,
snapshot immutability, restart recovery, and a caller-owned lifecycle handoff. Keep it batch-focused until the proving
lane produces positive evidence.

## Future Support Bar

A deferred feature can become supported only when Structure defines public DSL or configuration, backend capabilities,
compile-time diagnostics, explain output showing state assumptions, execution and generated-code behavior where both
apply, live Spark Structured Streaming verification, and troubleshooting guidance.
The following remain deferred or caller-owned:
- keep lifecycle ownership explicit until a separate product decision establishes operational semantics;
- stateful chains outside the two admitted finite pairs;
- cross and anti stream joins;
- selected-row helpers on streaming inputs;
- ranking, lag/lead, and rolling windows; and
- Dataset/Scala state APIs, broader processor combinations, and legacy Pandas forms awaiting their own evidence.

## V10 Adoption

The adopted streaming slices are governed by grouped plans rather than one plan per source document:

- `docs/dev/past/planning/P08022602.V10-streaming-state-and-join-contracts.plan.md`;
- `docs/dev/past/planning/P08022603.V10-streaming-side-effects-and-arbitrary-state.plan.md`; and
- `docs/dev/past/planning/P08022604.V10-evidence-catalog-reconciliation-and-hardening.plan.md`.

V10 keeps the permanent caller-owned lifecycle boundary. Sources, sinks, triggers, checkpoints, output modes,
start/stop, deployment, recovery, and external side effects remain outside ordinary Structure transforms.
