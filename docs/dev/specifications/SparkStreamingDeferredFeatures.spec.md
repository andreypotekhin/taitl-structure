# Spark Streaming Deferred Features

This specification records streaming behavior outside the admitted transformation surface. Current operation rules
are in [Streaming compatibility](StreamingCompatibility.spec.md) and the
[Streaming reference](../../reference/Streaming.ref.md). Per-family open evidence is maintained in
[Streaming gates](../gated/Streaming.gates.md).

## Caller-owned lifecycle

Structure must not generate or manage sources, writers, triggers, checkpoint locations, query names, writer output-mode
application, query start/stop, deployment, or recovery. These concerns remain caller-owned for every transform class.
The lifecycle capability names remain unavailable to ordinary compiled transformation bodies:

```text
streaming.source_generation
streaming.sink_generation
streaming.trigger_policy
streaming.checkpoint_policy
streaming.output_mode
streaming.query_lifecycle
```

Typed `foreach` and `foreach_batch` markers capture final-output handoffs rather than executing effects. A handoff
contains the output DataFrame and consumed schema. The caller constructs a writer or selects a batch transform,
validates its safety declarations, attaches the native writer/callback, and owns checkpointing, retries, credentials,
and idempotence. No exactly-once effect guarantee follows from a safety declaration. The normative handoff rules are
in [Retained-gate contracts](V11RetainedV9DesignGates.spec.md).

## Implemented transformation exceptions

Watermarked event-time and static session-window aggregation, supported dedupe and stream joins, two finite stateful
pairs, row processors on ordinary 4.1, and Pandas processors on ordinary 4.0/4.1 have their own admitted contracts.
Their presence does not imply general stateful chaining or lifecycle support. Scalar Python UDF streaming and exact-4.1
Arrow scalar/iterator streaming are also admitted independently; grouped Arrow aggregates/windows remain batch-only.

Each admitted family must state output modes, watermark requirements, state assumptions, target profiles, and live
online/generated/restart evidence where applicable. The latest full-lane release results remain separate from
feature-specific admission. Legacy `apply_in_pandas_with_state` has an implemented compiler path and accumulation/
restart evidence, while timeout and zero/multiple-output proof remains gated under its own plan.

## Deferred or excluded shapes

- Stateful chains outside the documented event-time-window pair and watermarked dedupe-to-window pair remain gated.
  Reversed order, joins/processors in a chain, and third stateful stages need explicit watermark, retention,
  output-mode, budget, and checkpoint semantics.
- Cross/anti stream-stream joins and additional join shapes remain excluded until their own finite-state evidence.
- Global selected-row helpers, priority selection, relation ordering/limits/offsets, ranking, lag/lead, and rolling
  projection windows remain streaming-ineligible. Materialize to batch or use an admitted finite event-time aggregate.
- Streaming missing-column union retains its nested-schema, nullability, alias, and state-evolution gate.
- Dataset/Scala arbitrary-state APIs without a PySpark entry point remain outside the PySpark plugin surface.
- Automatic state/checkpoint migration, general native processor methods beyond typed declarations, custom sink
  execution inside transforms, and certification of arbitrary hook effects remain caller-owned or separately gated.
- Spark Connect streaming has no support claim. A Connect batch result does not promote a streaming form.

Rejection must use a backend capability or streaming diagnostic naming the operation and corrective path. The compiler
must not silently fall back to raw SQL, a hook, local collection, RDD conversion, or a UDF. Explicit native processor
bindings and UDF declarations retain their own documented behavior.

## Admission evidence

A new shape requires a normative public interface and state/cardinality contract, capability rules and diagnostics,
explain/traceability behavior, generated-source checks, online/generated parity, live target evidence, and relevant
restart coverage. If the feature is already implemented but evidence-gated, document the implemented subset and missing
proof explicitly. Update the public reference, compatibility and machine-readable ledgers, gate register, feature plan,
and troubleshooting before claiming support.

Lifecycle ownership can change only through a separate product decision defining operational semantics and recovery;
implementing a typed transformation or handoff does not change this rule.
