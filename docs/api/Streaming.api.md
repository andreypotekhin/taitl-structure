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
  the caller owns idempotence, credentials, failure observation, and recovery. The supported profiles are listed in
  the [compatibility ledger](../compatibility/Streaming.compat.md). See the [row-level foreach contract](../dev/specifications/V11RetainedV9DesignGates.spec.md#row-level-foreach),
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

## Stateful Operations And Composition

Structured Streaming keeps state between input batches for operations such as aggregations, deduplication, joins, and
state processors. A transform may contain one admitted stateful operation followed by stateless work. Additional
stateful operations are rejected unless Structure has a specific finite contract for that combination.

- Cross and anti stream-stream joins are not supported because their completion and retention behavior cannot currently
  be bounded by Structure.
- Row `transform_with_state(...)` is available on ordinary PySpark 4.1. The separate Pandas
  `transform_with_state_in_pandas(...)` and legacy `apply_in_pandas_with_state(...)` operations have distinct processor
  APIs and runtime requirements; see the [compatibility ledger](../compatibility/Streaming.compat.md) for their
  supported profiles. Spark and the caller own native state and checkpoint evolution. Structure does not migrate
  persisted state.
- Typed row state processors use `StateProcessor[Input, Key, Output]`; declare named `ValueState[Schema]`,
  `ListState[Schema]`, and `MapState[KeySchema, ValueSchema]` attributes on the processor. They access handles through
  `self` in `on_rows`, `on_timer`, and optional `on_initial_state`. The immutable factories `value_state(...)`,
  `list_state(...)`, and `map_state(...)` accept optional `name=` and `ttl=datetime.timedelta(...)`. TTL requires
  `time_mode="ProcessingTime"`. Typed initial state requires both a concrete `on_initial_state` callback and the
  operation's `initial_state=` relation. Changing persisted state identity or schema is checkpoint-sensitive; use a
  new checkpoint after such a change because Structure does not migrate Spark state.
- Use `external_state_processor(...)` when processor code needs Python constructs or native PySpark features outside
  this typed interface.
- Example: declare typed handles on the processor and use them from a compiler-visible transform step. The example
  shows all three handle kinds; the processor body runs on Spark workers.

```python
from collections.abc import Iterator
from datetime import timedelta

from structure import *
from structure.plugin.pyspark import *


class Event(Schema):
    account_id = string(nullable=False)
    amount = integer(nullable=False)


class AccountKey(Schema):
    account_id = string(nullable=False)


class TotalState(Schema):
    total = integer(nullable=False)


class AmountKey(Schema):
    amount = integer(nullable=False)


class AmountCount(Schema):
    count = integer(nullable=False)


class TotalOutput(Schema):
    account_id = string(nullable=False)
    total = integer(nullable=False)


@state_processor
class AccountTotals(StateProcessor[Event, AccountKey, TotalOutput]):
    total: ValueState[TotalState] = value_state(ttl=timedelta(hours=1))
    recent: ListState[Event]
    by_amount: MapState[AmountKey, AmountCount] = map_state(name="counts_by_amount")

    def on_rows(
        self,
        key: AccountKey,
        rows: Iterator[Event],
        timers: TimerContext,
    ) -> Iterator[TotalOutput]:
        current = self.total.get()
        total = 0 if current is None else current.total
        for row in rows:
            total += row.amount
            self.recent.append_value(row)
            amount_key = AmountKey(amount=row.amount)
            previous = self.by_amount.get_value(amount_key)
            count = 1 if previous is None else previous.count + 1
            self.by_amount.update_value(amount_key, AmountCount(count=count))
        self.total.update(TotalState(total=total))
        yield TotalOutput(account_id=key.account_id, total=total)


class AccountTotalsTransform(Transform):
    events = input(Event, streaming=True)
    totals = output(TotalOutput)

    @step(input=events, output=totals)
    def accumulate(self, event: Event) -> TotalOutput:
        return transform_with_state(
            key=event.account_id,
            processor=AccountTotals,
            output_mode="Update",
            time_mode="ProcessingTime",
        )
```

- `PandasStateProcessor[Input, Key, State, Output]` uses the separate
  `on_batches(self, key, batches, state, timers)` callback. It receives Pandas batches, a typed `ValueState[State]`,
  and timer context, then yields Pandas output frames. The `@pandas_state_processor` decorator is an optional declaration
  validator. Both Spark 4 state processor APIs require pandas, PyArrow, and protobuf on the driver and workers; the
  Pandas API has the same dependencies on PySpark 4.0.
- General Pandas, RDD, and `mapInPandas` boundaries remain unsupported because they are not part of these typed state
  processor surfaces.

### Caller-owned arbitrary-state metadata

`ArbitraryStateContract` is an optional helper for application code that uses Spark's state APIs directly. It checks
declared schemas, grouping keys, timeout and initialization choices, and checkpoint/restart assumptions. It does not
run a processor, start a query, or migrate checkpoint data. You do not need it when using `transform_with_state(...)`.

## Compatibility

See the exhaustive [Streaming compatibility ledger](../compatibility/Streaming.compat.md).
