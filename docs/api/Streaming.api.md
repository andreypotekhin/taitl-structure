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
  PySpark 4.0 supports parsing, extraction, schema inspection, object conversion, JSON-null testing,
  validated `variant_literal(...)` extraction, watermarked `schema_of_variant_agg`, and typed inner/outer TVF expansion;
  PySpark 3.5 fails through the standard capability diagnostic before execution. PySpark 4.2-only helpers such as
  `is_valid_variant(...)` remain capability-gated until a 4.2 live lane exists.
- `event_time_between(...)` supplies the bounded event-time relation required by supported stream-stream joins.
- `streaming=True` declares the hook safe for its stated streaming shape; Structure does not inspect hook code.

## Lifecycle Boundaries

Structure returns a transformed DataFrame; the application creates sources and owns writers, checkpoints, triggers,
query start/stop, and recovery. Generated modules do not start queries or attach external sinks.

A transform may declare a row sink with `sink(WriterClass)` and `foreach(row, sink)`, or a schema-based batch sink with
`sink(Schema)` and `foreach_batch(row, sink)`. The caller attaches the returned handoff with PySpark's `foreach(...)` or
`foreachBatch(...)`. See the [Streaming reference](../reference/Streaming.ref.md) for the
[batch-transform handoff](../reference/Streaming.ref.md#attach-a-batch-transform-to-a-foreachbatch-sink),
[caller-controlled `foreachBatch`](../reference/Streaming.ref.md#attach-a-caller-controlled-foreachbatch-sink), and
[row-level sink](../reference/Streaming.ref.md#attach-a-transform-declared-row-level-sink) examples.

## Application-Controlled Side-Effect Safety

The example-app helper `examples.streams.adoption.ForeachBatchSafety` lets the application declare the sink identity,
idempotence key, retry policy, and snapshot identity before starting a `foreachBatch` query. It makes retry assumptions
explicit; the application remains responsible for honoring them. See the
[side-effect guidance](../reference/Streaming.ref.md#attach-a-caller-controlled-foreachbatch-sink).

## Stateful Operations And Composition

Structured Streaming keeps state between input batches. Structure admits one stateful operation followed by stateless
work unless a specific bounded combination is documented. Cross and anti stream-stream joins remain unsupported.

Row `transform_with_state(...)` supports ordinary PySpark 4.1. A typed processor declares named
`ValueState[Schema]`, `ListState[Schema]`, or `MapState[KeySchema, ValueSchema]` attributes on
`StateProcessor[Input, Key, Output]`. Its required callback is `on_rows(self, key, rows, timers)`; it may also implement
`on_timer(...)` and `on_initial_state(...)`. An initial-state callback must be paired with the operation's
`initial_state=` relation. Declaration factories can set a persisted `name=` or `timedelta` TTL; TTL requires
`time_mode="ProcessingTime"`. State changes may make an existing checkpoint incompatible, and Structure does not
migrate persisted state.

A transform step calls the operation like this:

```python
def accumulate(self, event: Event) -> TotalOutput:
    return transform_with_state(
        key=event.account_id,
        processor=AccountTotals,
        output_mode="Update",
        time_mode="ProcessingTime",
    )
```

Use `external_state_processor(...)` when processor code needs Python constructs or native Spark features outside the
Structure typed interface. For Pandas state processing, `PandasStateProcessor[Input, Key, State, Output]` implements
`on_batches(self, key, batches, state, timers)`, receiving Pandas batches and a typed `ValueState[State]` and yielding
Pandas output frames. The state processor APIs have separate runtime profiles and dependencies; see the
[compatibility ledger](../compatibility/Streaming.compat.md). The [Streaming reference](../reference/Streaming.ref.md#use-state-processors)
contains the complete typed example and state-operation details.

### Caller-owned arbitrary-state metadata

`ArbitraryStateContract` is an optional helper for application code that uses Spark's state APIs directly. It checks
declared schemas and checkpoint assumptions but does not run a processor or manage query lifecycle. It is not needed for
`transform_with_state(...)`.

## Compatibility

See the exhaustive [Streaming compatibility ledger](../compatibility/Streaming.compat.md).
