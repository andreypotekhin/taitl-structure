# Streaming API

Structure supports a conservative, application-controlled Structured Streaming shape. These declarations and helpers
classify compatibility and compile to streaming-safe DataFrame transformations when their documented conditions are met.
Examples abbreviate `order` as `o` and a second streaming relation as `c`.

For practical usage, see the [Streaming reference](../reference/Streaming.ref.md). This page remains the compact API
catalog; the [Streaming background](../background/Streaming.back.md) explains the state and lifecycle rationale.

## Streaming Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `input(..., streaming=True)` | Streaming input | `input(OrderRaw, streaming=True)` |
| `@transform(streaming=True)` | Compatibility enforcement | `@transform(streaming=True)` |
| `StreamingOutputMode` | Structured Streaming output mode | `mode = StreamingOutputMode.APPEND` |

**Details And Differences**

- `streaming=True` declares a streaming input; omitting it (or setting `False`) declares a static input.
- `@transform(streaming=True)` is an all-step streaming-capability contract. Streaming input declarations and
  composed streaming outputs trigger compatibility analysis, but do not implicitly set transform options.
- In a composed transform, the default boundary policy propagates streaming lineage through compiler-visible,
  compatible undeclared downstream code. Set `stream_to_batch_policy = "strict"` to require explicit
  `streaming=True` or `allow_stream_to_batch=True`. The allowance cannot suppress a known `STREAM-E0801`, and
  explicit `streaming=False` always remains a compilation error.
- `StreamingOutputMode` is the typed vocabulary used when explain output reports a caller-required output mode.

## Streaming Operations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `watermark(...)` | `withWatermark` | `watermark(o.event_time, delay="10 minutes")` |
| `window(event_time, duration, slide=None, start=None)` | `functions.window` | `window(o.event_time, "10 minutes")` |
| `window_time(window_value)` | `functions.window_time` | `window_time(first_window.bucket)` |
| `session_window(event_time, gap)` | `functions.session_window` | `session_window(o.event_time, "5 minutes")` |
| `drop_duplicates(...)` | `dropDuplicates` / `dropDuplicatesWithinWatermark` | `drop_duplicates(o.id)` |
| `drop_duplicates_within_watermark(...)` | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(o.id)` |
| `budget(...)` | Structure state budget declaration and progress guard metadata | `budget(max_rows=500_000, max_state_bytes=268_435_456)` |
| `@special(type="udf")` | scalar PySpark `udf` | `self.normalize(o.id)` |
| `event_time_between(...)` | Stream-stream time-range predicate | `event_time_between(o.at, c.at, upper="1 hour")` |
| `@raw(..., streaming=True)` | Streaming-safe hook | `@raw(streaming=True)` |

**Details And Differences**

- `watermark(...)` is a compiled-step operation with explicit field and delay.
- `window(...)` retains its keyword-only analytical `WindowSpec` form. Its positional event-time form is a grouping key;
  one call cannot mix the two argument families, while separate calls may use either form in the same transform.
- Event-time windows return `Struct[TimeWindow]` with non-null `start` and `end` timestamps. Tumbling and sliding
  aggregates require a preceding watermark on that same event-time field and use application-applied `append` or
  `update` mode.
- `window_time(...)` accepts only a `TimeWindow` produced by `window(...)` and is supported for one chained pair:
  a watermarked first event-time window aggregate, stateless work, then a second
  `window(window_time(first_window), ...)`
  aggregate. The generated transform uses public `functions.window_time`; broader chained stateful operations remain
  rejected with `STREAM-E0801`.
- `session_window(...)` requires a preceding watermark on the same event-time field, a static positive gap, one
  ordinary grouping key in addition to the session key, and application-applied `append` mode. Dynamic gaps remain
  deferred.
- Broad analytic windows and global selected-row helpers are streaming-ineligible. For finite event-time selection,
  use grouped `first_value(...)` or `last_value(...)` inside a watermarked `window(...)`; these preserve the typed
  selected value and lower through public `min_by(...)`/`max_by(...)`. The existing `latest_by(...)` and
  `earliest_by(...)` relation helpers remain batch-only.
- `drop_duplicates(...)` remains cross-mode: batch lowers to `dropDuplicates`, while a streaming frame lowers to
  watermark-bounded `dropDuplicatesWithinWatermark`. `drop_duplicates_within_watermark(...)` makes that streaming-only
  choice explicit and requires `streaming=True` plus a preceding watermark.
- On ordinary PySpark 3.5 and 4.0, one watermarked `drop_duplicates_within_watermark(...)` followed by one watermarked
  event-time `group_by(window(...))` aggregate is supported in Append mode. Declare operator limits with
  `budget(max_rows=..., max_state_bytes=...)` immediately after each stateful helper. Other chained stateful shapes
  remain rejected with `STREAM-E0801`; processor chains are not included.
- `state_budget_checking` accepts `off`, `compile_time_check`, `require_declaration`, or
  `declaration_and_runtime`. The default checks declarations at compile time without requiring them. Runtime limits are
  observed only after Spark reports a completed batch; the caller attaches `session.state_budget_guard(result)` to its
  own query and checks the handle. This does not provide a hard within-batch cap.
- Scalar `@special(type="udf")` expressions are admitted as row-local ordinary-PySpark streaming transformations.
  They retain the existing `warn_on_udfs` warning policy and remain unavailable on Spark Connect.
- Variant fields and helpers are admitted as profile-gated streaming transformations on ordinary PySpark 4 profiles.
  PySpark 4.0 live evidence covers parsing, extraction, schema inspection, object conversion, JSON-null testing,
  validated `variant_literal(...)` extraction, watermarked `schema_of_variant_agg`, and typed inner/outer TVF expansion;
  PySpark 3.5 fails through the standard capability diagnostic before execution. PySpark 4.2-only helpers such as
  `is_valid_variant(...)` remain capability-gated until a 4.2 live lane exists.
- `event_time_between(...)` supplies the bounded event-time relation required by supported stream-stream joins.
- `streaming=True` declares the hook safe for its stated streaming shape; Structure does not inspect hook code.

## Stateful Composition And Deferred State

The compiler records state-stage metadata for admitted aggregates, bounded deduplication, and bounded stream-stream
joins, including watermarks, grouping or join keys, retention bounds, and required output modes. This metadata makes the
state assumptions visible in explain output; it does not make Structure control query lifecycle or recovery.

- The supported composition boundary is one admitted stateful operation followed by stateless work. A second stateful
  operation remains rejected with `STREAM-E0801` unless a specific finite contract is admitted.
- Cross and anti stream-stream joins remain rejected until finite completion, retention, and restart behavior are
  proven.
- Arbitrary state APIs remain design-gated pending profile-specific live evidence. The compiler surface includes
  `transform_with_state(...)` for ordinary PySpark 4.1 and `transform_with_state_in_pandas(...)` for ordinary PySpark
  4.0 and 4.1; both 4.1 forms require pandas, PyArrow, and protobuf on the driver and workers. Neither API currently
  carries a Structure support claim. The initial 4.1 integration lane runs V11 tests only; the row operation still
  requires typed/native timer, online/generated parity, and same-checkpoint restart evidence. The separate
  `apply_in_pandas_with_state(...)` operation targets ordinary PySpark 3.5, 4.0, and 4.1 through the legacy
  `GroupedData.applyInPandasWithState`; its support claim awaits matching profile evidence. See the
  [arbitrary-state contract](../dev/specifications/V9StreamingDesignGatedFeatures.spec.md#arbitrary-state-apis) and
  the [state gate](../dev/gated/Streaming.gates.md#arbitrary-state-processors--design-gated).
- Typed state schemas come from the specialized `StateProcessor[Input, Key, State, Output]` or
  `PandasStateProcessor[Input, Key, State, Output]` base, including specialized intermediate classes. The
  `@state_processor` and `@pandas_state_processor` decorators remain optional compatibility validators.
- The typed row processor uses one `ValueState`; `TimerContext` exposes timer registration, deletion, listing, current
  processing time, and the current watermark in milliseconds. The watermark property requires a watermarked input. Use
  `external_state_processor(...)` when the Spark processor needs additional state types, multiple named states, TTL, or
  initial-state handling. These typed callbacks remain behind the live evidence gate above.
- General Pandas, RDD, and `mapInPandas` boundaries remain unsupported because they are not part of these typed state
  processor surfaces.

## Lifecycle Boundaries

Supported transform shapes include row-local projection/filter (including scalar Python UDFs), stream-static left/inner
joins and `exists(...)` filtering, event-time and session-window aggregation, bounded dedupe, bounded inner
stream-stream joins, and bounded left/right/full outer and semi stream-stream joins. The application controls
`readStream`, `writeStream`, checkpoints, triggers, output-mode application, query lifecycle, and side effects.
`foreachBatch` has application-controlled guidance through the streams adoption helper; generated Structure modules
must not emit `foreachBatch`. A transform may declare a typed row-level sink on a final output, then return a read-only
handoff on its result:

```python
from structure.plugin.pyspark import Sink


class AlertWriter(Sink):
    def __init__(self, destination: str) -> None:
        self.destination = destination

    def process(self, row: Row) -> None:
        write_alert(self.destination, row)


class PublishAlerts(Transform):
    events = input(Event, streaming=True)
    alerts = output(Alert)
    publish_alerts = sink(AlertWriter)

    @step(output=alerts)
    def publish(self, event: Event, sink: AlertWriter) -> Alert:
        alert = Alert(id=event.id)
        foreach(alert, sink)
        return alert


result = PublishAlerts(events=events).run(session)
handoff = result.publish_alerts
assert handoff.dataframe is result.alerts
query = handoff.dataframe.writeStream.foreach(
    handoff.writer(destination="alerts-service")
).option("checkpointLocation", foreach_checkpoint).start()
```

`Sink` is imported from `structure.plugin.pyspark`. Its subclasses are opaque during compilation and implement
`process(row: Row) -> None`; no `@special(type="opaque")` decorator is needed. `foreach(row, sink)` records a binding
to the exact row returned by the step; it does not execute the writer or start a query. The sink must resolve to a
declared final output; intermediate rows and sink-bearing composed or staged
transforms fail with `DSL-E0406`. The handoff keeps the writer class so the caller can supply application settings.
Batch callers invoke `handoff.dataframe.foreach(handoff.writer(...).process)`; batch writers may not define streaming
`open` or `close` methods. Streaming writers must be noncallable and define `process(row)`; optional `open` and `close`
follow PySpark's partition/epoch lifecycle. The caller starts and stops each query. An added streaming sink is a second,
independent query with its own checkpoint and progress. Retries and checkpoint restarts may repeat external writes, so
the caller owns idempotence, credentials, failure observation, and recovery. Live evidence covers classic PySpark 3.5
and 4.0; Spark Connect is unclaimed. See the [row-level foreach contract](../dev/specifications/V11RetainedV9DesignGates.spec.md#row-level-foreach),
[Spark Streaming](../dev/specifications/SparkStreaming.spec.md), and the
[Execution reference](../background/Execution.back.md).

## Application-Controlled Side-Effect Safety

Before starting a `foreachBatch` sink, the application provides a `ForeachBatchSafety` declaration with a stable
`sink_identity`,
an `idempotence_key` such as `snapshot_id:batch_id`, a `retry_policy` (`at_least_once`, `idempotent`, or
`transactional`), and a stable `snapshot_id`. The adoption helper rejects missing or unknown declarations before
calling `start()`. These declarations make the recovery assumptions reviewable; they do not make callback code
idempotent, transactional, or secure. The callback and its sink remain the application's responsibility, including using
the declared key, handling retries, and ensuring that the checkpoint and snapshot identity remain compatible.

## Typed Arbitrary-State Contract

Arbitrary state remains design-gated; `ArbitraryStateContract` is a metadata completeness guard, not a state processor
runtime. The four ledger families are independent: row-based `transform_with_state` targets ordinary PySpark 4.1;
`transform_with_state_in_pandas` targets ordinary PySpark 4.0 and 4.1; `apply_in_pandas_with_state(...)` targets
ordinary PySpark 3.5, 4.0, and 4.1; Dataset/Scala arbitrary-state APIs remain outside the V11 claim. The row family has
the dedicated [admission and typed-parity plan](../dev/planning/P10062603.V11-transform-with-state-admission-and-typed-parity.plan.md);
the Pandas families retain their separate plans. Each remains gated until its own online/generated parity, timer or
callback behavior, and same-checkpoint restart evidence passes. `apply_in_pandas_with_state(...)` has a separate
typed/native compiler path and legacy `PandasGroupState` facade; it does not adapt Spark 4 processor callbacks or
migrate their checkpoint state. Before reviewing another native state API, the contract
records typed input, key, state, and output Schemas; grouping fields; timeout policy, clock, and duration;
initialization, update, and removal behavior; target PySpark profile; hook boundary; checkpoint identity; serialized
state version; and restart policy. `contract.validate()` rejects missing or inconsistent declarations with
`ARBITRARY-STATE-E0901`, `ARBITRARY-STATE-E0902`, or `ARBITRARY-STATE-E0903`.

Validation does not start a query, generate a state processor, control a checkpoint, or prove recovery. The application
still controls the native PySpark API and live restart evidence. A passing row-state proof does not promote the Pandas,
legacy, Dataset, or Scala families. Structure must not promote any streaming ledger row until its separate runtime
contract and target-profile evidence exist.

## SearchDocuments Streaming Status

SearchDocuments declares streaming inputs but remains `batch_only` because its current ranking, deduplication, and join
shapes are not bounded for Structured Streaming. Its future streaming work is deferred until the compiler and Spark
integration lanes can prove bounded ranking state, finite event-time completion, append-only output, and checkpoint
restart. The current Search transform does not expose a caller-adoption contract or start a streaming query. See the
retained requirements in
[search streaming plan](../dev/planning/P08022605.SearchDocuments-structured-streaming.plan.md).

## Compatibility

See the exhaustive [Streaming compatibility ledger](../compatibility/Streaming.compat.md).
