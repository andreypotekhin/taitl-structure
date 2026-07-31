# V9 Streaming Design Gates

## Purpose

This design turns the v9 streaming `design-gated` rows into schedulable work. It is part of the broader
[V9 API Catalog Design Gates](V9ApiCatalogDesignGates.md) program. A design-gated row is not a vague backlog item: it
is an API family that may become Structure-supported only after its state, lifecycle, diagnostics, generated-code, and
live Spark evidence are explicit.

The governing rule stays unchanged. Structure transforms return PySpark DataFrame plans. They do not own
`readStream`, `writeStream`, checkpoint locations, triggers, query start/stop, deployment, recovery, or side effects
unless a later product decision creates a separate lifecycle-owning runtime.

Within that boundary, v9 is support-first, as recorded in
[D07302603](decisions/D07302603.V9-streaming-support-first.md). A design-gated streaming row is a proving lane:
prototype the typed transform or executable caller-owned adoption shape, gather target evidence, and only then keep the
row gated or mark it `streaming-ineligible` if Spark semantics or lifecycle ownership make support unsafe.

## Design-Gated Families

V9 has five streaming design gates:

- chained event-time window aggregation;
- chained stateful operators;
- selected-row helpers over streaming input;
- analytic window projections over streaming input;
- side-effect and arbitrary-state APIs, including `foreach`, `foreachBatch`, `applyInPandasWithState`, and
  `transformWithState`.

The first two are closely related. Spark can support some chained event-time aggregation shapes when the second stage
uses the first stage's window boundary as event time. Structure can admit only the exact forms it can describe in IR.
The remaining families need either bounded state design or lifecycle/side-effect design before they can move.

## Shared Admission Model

Every admitted streaming state feature must carry these facts in compiler-visible IR:

- the event-time field or derived event-time expression;
- the watermark that bounds state;
- the grouping key or partition key;
- the stateful operation family;
- the output mode the caller must apply;
- whether another stateful operation may follow;
- the diagnostic shown when the shape is unsafe;
- the generated PySpark form, using public DataFrame and Column APIs only.

The compiler should reject unsupported streaming shapes before query start. Rejection is part of the feature, not a
failure to implement it.

## Implementation Order

Resolve the streaming gates in this order:

1. Prototype `window_time(...)` directly in raw PySpark on the PySpark 3.5 and 4.0 profiles.
2. Add state-stage metadata so diagnostics can name the first stateful operation and the rejected second stateful
   operation.
3. Admit only chained event-time window aggregation if both targets prove the generated form, output mode, and restart
   behavior.
4. Prototype selected-row alternatives only inside finite event-time or session windows.
5. Specify and prototype finite analytic-window alternatives before classifying the broad batch projection helpers as
   streaming-ineligible.
6. Add caller-owned `foreachBatch` guidance only outside transform methods and generated transform modules, then decide
   whether row-level `foreach` can receive the same caller-owned treatment.
7. Draft the typed arbitrary-state programming model needed for `applyInPandasWithState` and `transformWithState`.

## Chained Event-Time Window Aggregation

Chained window aggregation means a transform performs an event-time window aggregation and then aggregates those window
results again by a larger window. Spark's public `window_time(...)` function converts a `window(...)` struct back into
a timestamp that a following `window(...)` can consume.

Structure should introduce a typed `window_time(window_field)` expression only for a `TimeWindow` struct produced by
the streaming window helper. The IR must preserve that the timestamp came from a completed window boundary; it must not
be a generic cast or raw field extraction that lets arbitrary structs masquerade as event time. The second-stage
`window(...)` may consume that expression. The accepted shape is narrow: one watermarked input, one first-stage
event-time aggregate, a stateless projection, and one second-stage event-time aggregate over `window_time(...)`.

The design rejects arbitrary `window(window(...))` nesting, dynamic gap inheritance, chained session windows, and any
third stateful operator. The caller owns `append` mode unless the concrete Spark evidence proves `update` is safe for a
specific admitted shape.

## Chained Stateful Operators

The current v9 policy admits one stateful operator followed by stateless operations. The next safe expansion is not
"any two stateful operators"; it is a named chain contract. A stateful operator is an operation that keeps Spark state
across micro-batches, such as watermarked aggregation, bounded dedupe, session aggregation, or stream-stream join.

Structure should model state stages explicitly. A first implementation should admit only the chained window case above.
All other second-stateful attempts remain rejected with a diagnostic that names the earlier stateful operation and the
new operation that would create the chain. This keeps the public rule conservative while giving users one common,
Spark-supported rollup path.

A state stage should record, in order, the operation family, event-time source, watermark source, grouping keys,
output-mode requirement, and whether a following stateful operation is permitted. Explain output should show the stage
list rather than a single boolean such as "has state", because users need to see why a later operation was rejected.

## Selected-Row Helpers

Batch helpers such as `latest_by(...)`, `earliest_by(...)`, `dedupe_latest_by(...)`, and
`dedupe_earliest_by(...)` lower through ordered selection. On an unbounded stream, a global "latest row per key" is
not finite without a watermark, event-time bound, timeout, or window.

The streaming design should split selected-row helpers into two classes:

- window-scoped selected rows, admitted only inside a watermarked event-time or session window and lowered as grouped
  aggregation over a typed struct;
- global selected rows, rejected as a batch-materialization boundary unless an arbitrary-state contract is approved.

Tie policy must be explicit. A streaming selected-row helper cannot default to arbitrary row choice. If two rows have
the same order key and no secondary order key, the helper must either reject at compile time when the tie cannot be
represented or emit a runtime assertion with a documented diagnostic.

## Analytic Window Projections

Analytic projections include `row_number(...)`, `rank(...)`, `dense_rank(...)`, `lag(...)`, `lead(...)`, and rolling
window aggregates over `Window.partitionBy(...).orderBy(...)`. These are batch DataFrame window projections, not Spark
Structured Streaming event-time grouping windows.

The v9 design should not admit broad analytic projection windows over streaming input. Instead, it should prototype
the few user-facing intents behind them:

- top-N per finite event-time window;
- previous/next value within a finite event-time window;
- rolling aggregate over a finite event-time window.

If Spark rejects these plans or the generated form requires arbitrary state, the catalog row should move from
`design-gated` to `streaming-ineligible` for the broad batch helper and point users to either the supported finite
streaming alternative or caller-owned PySpark after a materialization boundary.

The prototype must not use Spark actions, collection, RDDs, raw SQL strings, private JVM state, or lifecycle APIs to
simulate bounded state. Those forms would prove only caller-owned PySpark feasibility, not Structure transform support.

## Foreach And Side Effects

`foreach` and `foreachBatch` are side-effect APIs. They control writes, retries, idempotence, failure handling, and
recovery. That is lifecycle policy, not transformation logic.

V9 should not admit these inside transform methods. The design path is a caller-owned side-effect recipe first:
Structure returns the transformed DataFrame, and the caller applies `foreachBatch` in the same layer that owns
checkpoint, trigger, output mode, and query lifecycle. Any future Structure-owned side-effect API must require an
idempotence key, retry policy, sink identity, checkpoint policy, and failure diagnostic before implementation.

The first useful v9 outcome is now admitted as caller-owned guidance: application code can call
`examples.streams.adoption.start_foreach_batch_query(transformed, callback, ...)` after Structure returns a transformed
streaming DataFrame. This is not Structure-owned side-effect support. Generated transform modules must remain free of
`foreach`, `foreachBatch`, `writeStream`, `start`, checkpoint configuration, triggers, and output sink calls.

Row-level `foreach` stays design-gated. It runs per-row callbacks and needs a tighter sink identity, idempotence, retry,
security, and recovery model before Structure should provide any public support around it.

## Arbitrary State APIs

Arbitrary state APIs include `applyInPandasWithState`, `transformWithState`, and related state processor APIs. They
require typed state schema, timeout semantics, version-specific PySpark support, recovery behavior, and user code that
can mutate state.

The design path is a separate stateful-programming model, not a small transform helper. A future API must declare:

- input row schema;
- state schema;
- output schema;
- timeout policy;
- event-time or processing-time clock;
- state initialization and cleanup behavior;
- target PySpark version support;
- generated-code shape and hook boundary;
- live restart evidence.

Until those facts are explicit, arbitrary state stays design-gated and outside streaming-compatible transform support.
For v9 closeout, the expected outcome is at least an implementation-ready typed state-model specification rather than a
bare deferral.

## Public Documentation Rule

Public docs must distinguish three outcomes:

- `implemented`: Structure owns typed transformation support.
- `streaming-ineligible`: the shape requires batch materialization or violates Spark/Structure streaming constraints.
- `design-gated`: the shape has a written design and specification, but no Structure support claim yet.

Do not use `planned` or generic `deferred` in the APICatalog Streaming section. V9's value is precise accounting.
