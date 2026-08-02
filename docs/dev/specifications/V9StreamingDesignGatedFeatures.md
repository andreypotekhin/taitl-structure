# V9 Streaming Design-Gated Features

## Purpose

This specification records the v9 streaming design-gated contracts and their implementation status. The exact chained
event-time window shape is now `structure-supported`; the remaining rows retain their individual design-gated status.
Each contract defines the evidence required before a row can move from `design-gated` to `structure-supported`,
`caller-owned-guided`, or `streaming-ineligible`.

The extended v9 posture is support-first. Each row should attempt a typed Structure-supported transform contract or an
executable caller-owned adoption contract before it remains gated. `streaming-ineligible` is the result of evidence, not
the default.

The companion design is [V9StreamingDesignGates.md](../design/V9StreamingDesignGates.md). The broader catalog
specification is [V9ApiCatalogDesignGatedFeatures.md](V9ApiCatalogDesignGatedFeatures.md). The governing coverage
ledger is `src/structure/plugin/pyspark/resources/pyspark-streaming-api-coverage.json`.

## Status Vocabulary

Use these exact statuses for the design-gated families:

- `design-gated`: a design and implementation specification exist, but Structure does not yet support the API.
- `structure-supported`: Structure has typed source syntax, IR, online/generated lowering, diagnostics, docs, and live
  PySpark 3.5/4.0 evidence, or profile-specific positive and rejection evidence for target-gated APIs.
- `streaming-ineligible`: the feature requires batch materialization or violates the streaming transform contract.
- `caller-owned-guided`: Structure does not own the API, but executable examples and diagnostics show how caller code
  should use it around Structure transforms.

## Target-Gated Row-Local Streaming Helpers

When a row-local transformation is supported only on a newer PySpark profile, the streaming ledger must be target-aware.
For PySpark 4 Variant fields and helpers, v9 keeps the streaming support claim and must add:

- live PySpark 4.0 evidence that `variant(...)`, `parse_json(...)`, `try_parse_json(...)`,
  `schema_of_variant(...)`, `variant_get(...)`, `try_variant_get(...)`, `to_variant_object(...)`, and
  `is_variant_null(...)` work over caller-owned streaming input;
- PySpark 3.5 rejection evidence proving the capability diagnostic fires before execution;
- profile-gated compile evidence for PySpark 4.2-only helpers such as `is_valid_variant(...)` until a 4.2 live lane is
  available;
- public docs that state the streaming claim is profile-gated to resolved PySpark 4 ordinary profiles.

## Chained Event-Time Window Aggregation

Status: `structure-supported` for the exact two-stage event-time window shape described below. PySpark 3.5 and 4.0
live file-stream evidence covers both online and generated execution. Broader chained stateful operators remain
rejected.

The public API is:

```text
window_time(window_value)
```

`window_value` must be a typed `Struct[TimeWindow]` value produced by the existing streaming `window(event_time, ...)`
helper. The result is a timestamp expression representing the event time of the completed window and may be passed to a
second `window(...)` grouping key.

Accepted shape:

- exactly one streaming input before the first stateful operation;
- a compiler-visible `watermark(event_time, delay=...)` before the first window;
- a first-stage tumbling or sliding `window(event_time, duration, slide=None, start=None)` aggregation;
- only stateless projection/filter between the first and second aggregation;
- a second-stage `window(window_time(first_window), duration, slide=None, start=None)` aggregation;
- generated PySpark uses public `pyspark.sql.functions.window_time`;
- explain output reports caller-required `append` output mode unless target evidence proves a narrower alternative.

Implementation files should include the public expression helper, symbolic expression capture, PySpark online and
generated renderers, streaming compatibility classification, capability checks for PySpark 3.5/4.0, and the public
streaming API docs. Tests must include symbolic capture, generated source, online/generated parity where feasible,
classifier acceptance, classifier rejection, and live restart evidence for the admitted profile.

Rejected shapes:

- nested `window(window(...))` without `window_time(...)`;
- third or later stateful operation;
- second-stage dedupe, join, session window, arbitrary state, or selected-row helper;
- missing first-stage watermark;
- using `window_time(...)` on a non-window struct.

Diagnostics must use `STREAM-E0801` and name the missing `window_time(...)`, missing watermark, or unsupported second
stateful operation.

## Chained Stateful Operators

The first admitted chained stateful shape must be only chained event-time window aggregation. All other chained
stateful operators remain rejected until a separate specification adds them.

The compiler must record state-stage metadata for every streaming aggregate, dedupe, session window, and stream-stream
join. Each stage records operation family, event-time source, watermark source, grouping keys, required output mode,
and whether a following stateful operation is allowed. When a transform adds a second stateful operation, the checker
must decide whether the pair is the approved chained-window pair. If not, it fails before query start with a diagnostic
that names both stateful operations. Explain output must show the ordered state-stage list.

Acceptance for this gate requires tests that prove:

- the approved chained-window pair is compatible;
- aggregate followed by dedupe is rejected;
- dedupe followed by aggregate is rejected;
- stream-stream join followed by aggregate is rejected;
- generated source contains no lifecycle calls or Spark actions.

## Window-Scoped Selected-Row Helpers

The candidate support shape is selected row per key within a finite event-time grouping window.

Accepted candidate:

- the current DataFrame is streaming;
- a watermark exists on the event-time field;
- the helper is called inside a grouped aggregation whose grouping keys include `window(event_time, ...)`;
- `partition_by` keys are ordinary grouping keys or a subset of grouping keys;
- `order_by` is a deterministic list of scalar expressions;
- ties are resolved by an explicit secondary order key or rejected.

Implementation should first attempt a typed struct aggregate such as a deterministic max/min-by recipe if both PySpark
3.5 and 4.0 evidence prove streaming support. If Spark rejects the plan, the row may move to `streaming-ineligible`
for transform support only with that evidence, and the docs must say to materialize to batch before using selected-row
helpers.

Global selected-row helpers over unbounded streams are not a candidate for v9 support.

Diagnostics must distinguish global unbounded selection from finite-window selection. Global selection should explain
that no watermark/window bounds the state. Finite-window rejection should name the missing watermark, missing explicit
tie policy, unsupported aggregate recipe, or Spark target evidence gap.

## Analytic Window Projections

No broad analytic window projection is accepted by this specification. The v9 work should prototype finite-window
alternatives for the user-facing intents behind `row_number(...)`, `rank(...)`, `dense_rank(...)`, `lag(...)`, `lead(...)`,
`rolling_sum(...)`, `rolling_avg(...)`, `rolling_min(...)`, and `rolling_max(...)`. The existing batch helpers remain
batch-only over streaming input unless a later specification defines a finite state contract for that exact helper.

Any admitted finite alternative must use a new explicit API or option and must not silently reinterpret the existing
batch analytic-window helpers. Existing batch helpers keep their batch meaning; streaming callers should receive a
diagnostic unless they choose a future streaming-specific finite-window form.

Acceptance for this gate requires:

- live PySpark evidence plus a narrower admitted API with explicit watermark, frame, state, and output-mode rules; and
- a ledger update from `design-gated` to `streaming-ineligible` with diagnostics and public docs that direct users to a
  batch materialization boundary for any remaining broad helper.

## Foreach And ForeachBatch

`foreach` and `foreachBatch` must not be callable from Structure transform methods. The v9 implementation may add a
caller-owned recipe and generated-source scan coverage, but it must not place side-effect sinks inside generated
transform modules.

A future Structure-owned side-effect API requires a separate lifecycle-owning runtime specification with:

- sink identity;
- idempotence key;
- retry behavior;
- checkpoint policy;
- failure and recovery behavior;
- security review for user callback execution;
- live restart evidence.

V9 admits `foreachBatch` as `caller-owned-guided`, not Structure-owned. The executable recipe is
`examples.streams.adoption.start_foreach_batch_query(...)`, and tests prove it builds the ordinary PySpark writer chain
outside generated transform modules. Row-level `foreach` remains design-gated.

The caller-owned recipe is accepted only when it shows this shape:

- caller creates or receives a streaming DataFrame;
- Structure transform returns a transformed streaming DataFrame;
- caller applies `transformed.writeStream.foreachBatch(callback)` through the adoption helper with checkpoint, trigger,
  output mode, and query lifecycle in caller code;
- generated Structure modules contain no side-effect API calls.

## Arbitrary State APIs

Arbitrary state APIs remain design-gated until the typed state model exists. V9 should produce that implementation-ready
model before closeout. A future implementation must define the model before exposing
`applyInPandasWithState`, `transformWithState`, or any state processor API.

Required contract:

- state schema declared as a Structure `Schema`;
- input and output schemas declared as Structure schemas;
- event-time or processing-time timeout policy;
- state initialization behavior;
- state update and removal behavior;
- PySpark profile gating, because arbitrary state APIs differ across Spark versions;
- generated-code shape that does not hide Pandas/RDD conversion unless explicitly declared;
- restart evidence proving checkpointed state survives query restart.

Without this contract, arbitrary state cannot be implemented through `@raw(streaming=True)`. That annotation may allow
ordinary row-local PySpark expressions, but it does not authorize hidden state processors or user-managed checkpointed
state inside Structure transforms.

## Acceptance

The v9 design-gate follow-up is accepted when:

- this specification and the companion design are linked from the v9 coverage specification and streaming deferred
  feature docs;
- every design-gated ledger row has evidence paths pointing to the design/specification;
- every target-gated streaming-supported row has positive evidence for supported profiles and rejection evidence for
  unsupported profiles;
- the public APICatalog Streaming section uses `implemented`, `design-gated`, `streaming-ineligible`, or `unsupported`
  rather than `planned` or generic `deferred`;
- an ExecPlan exists under `docs/dev/planning/` that describes how to prototype, implement, test, and document each
  gate;
- guard tests fail if the catalog or ledger drifts away from this specification.
