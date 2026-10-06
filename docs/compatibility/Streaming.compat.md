# Streaming Compatibility

This is the compatibility companion to the [API reference](../api/Streaming.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Streaming and event-time helpers

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `input(...)` | Streaming input | `input(OrderRaw, streaming=True)` | yes | yes | Declares a transform input with an explicit Schema; `streaming=True` marks a streaming boundary for compatibility analysis. |
| `@transform(...)` | Compatibility enforcement | `@transform(streaming=True)` | yes | yes | `@transform(streaming=True)` is an all-step streaming-capability contract. Streaming input declarations and composed streaming outputs trigger compatibility analysis, but do not implicitly set transform options. |
| `StreamingOutputMode` | Structured Streaming output mode | `StreamingOutputMode` | yes | yes | `StreamingOutputMode` is the typed vocabulary used when explain output reports a caller-required output mode. |
| `watermark(...)` | `withWatermark` | `watermark(o.event_time, delay="10 minutes")` | yes | yes | `watermark(...)` is a compiled-step operation with explicit field and delay. |
| `window(...)` | `functions.window` | `window(o.event_time, "10 minutes")` | yes | yes | `window(...)` retains its keyword-only analytical `WindowSpec` form. Its positional event-time form is a grouping key; one call cannot mix the two argument families, while separate calls may use either form in the same transform. `window_time(...)` accepts only a `TimeWindow` produced by `window(...)` and is supported for one chained pair: a watermarked first event-time window aggregate, stateless work, then a second `window(window_time(first_window), ...)` aggregate. The generated transform uses public `functions.window_time`; broader chained stateful operations remain rejected with `STREAM-E0801`. Broad analytic windows and global selected-row helpers are streaming-ineligible. For finite event-time selection, use grouped `first_value(...)` or `last_value(...)` inside a watermarked `window(...)`; these preserve the typed selected value and lower through public `min_by(...)`/`max_by(...)`. The existing `latest_by(...)` and `earliest_by(...)` relation helpers remain batch-only. |
| `window_time(...)` | `functions.window_time` | `window_time(first_window.bucket)` | yes | yes | `window_time(...)` accepts only a `TimeWindow` produced by `window(...)` and is supported for one chained pair: a watermarked first event-time window aggregate, stateless work, then a second `window(window_time(first_window), ...)` aggregate. The generated transform uses public `functions.window_time`; broader chained stateful operations remain rejected with `STREAM-E0801`. |
| `session_window(...)` | `functions.session_window` | `session_window(o.event_time, "5 minutes")` | yes | yes | `session_window(...)` requires a preceding watermark on the same event-time field, a static positive gap, one ordinary grouping key in addition to the session key, and application-applied `append` mode. Dynamic gaps remain deferred. |
| `drop_duplicates(...)` | `dropDuplicates`, `dropDuplicatesWithinWatermark` | `drop_duplicates(o.id)` | yes | yes | `drop_duplicates(...)` remains cross-mode: batch lowers to `dropDuplicates`, while a streaming frame lowers to watermark-bounded `dropDuplicatesWithinWatermark`. `drop_duplicates_within_watermark(...)` makes that streaming-only choice explicit and requires `streaming=True` plus a preceding watermark. |
| `drop_duplicates_within_watermark(...)` | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(o.id)` | yes | yes | `drop_duplicates(...)` remains cross-mode: batch lowers to `dropDuplicates`, while a streaming frame lowers to watermark-bounded `dropDuplicatesWithinWatermark`. `drop_duplicates_within_watermark(...)` makes that streaming-only choice explicit and requires `streaming=True` plus a preceding watermark. |
| `@special(...)` | `udf` | `self.normalize(o.id)` | yes | yes | Scalar `@special(type="udf")` expressions are admitted as row-local ordinary-PySpark streaming transformations. They retain the existing `warn_on_udfs` warning policy and remain unavailable on Spark Connect. |
| `event_time_between(...)` | Stream-stream time-range predicate | `event_time_between(o.at, c.at, upper="1 hour")` | yes | yes | `event_time_between(...)` supplies the bounded event-time relation required by supported stream-stream joins. |
| `@raw(...)` | Streaming-safe hook | `@raw(streaming=True)` | yes | yes | Runs caller-authored PySpark outside the symbolic compiler contract; streaming hooks declare `streaming=True` for streaming-plan checks. |

## Row-level side sinks

Row-level `foreach` is a `caller-owned-guided` handoff for direct transforms. The Structure step associates a returned
final output row with a declared writer; the caller receives its output DataFrame and writer class and uses native
PySpark to run the batch action or start a second streaming query. This does not modify a query the caller has already
started.

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `sink(WriterClass)` + `foreach(row, sink)` | `DataFrame.foreach`, `DataStreamWriter.foreach` | `result.publish_alerts.dataframe.writeStream.foreach(writer).start()` | 3.5 only | 4.0 only | Live batch, writer lifecycle, independent-query, checkpoint-restart, and online/generated evidence covers classic PySpark 3.5 and 4.0. The writer subclasses `structure.plugin.pyspark.Sink`; the streaming instance must be noncallable and provide `process(row)`. Batch writers use `process` and cannot define `open` or `close`. Spark Connect is unclaimed. The caller owns writer construction, query lifecycle, separate checkpoints, retries, credentials, and idempotence. Composed or staged transforms with sinks are rejected with `DSL-E0406`. See the [row-level foreach contract](../dev/specifications/V11RetainedV9DesignGates.spec.md#row-level-foreach). |

## Stateful processor operations

These Structure compiler surfaces are separate from the shared PySpark 3.5/4.0 compatibility baseline. Their support
claims remain profile-gated until live processor, timer, online/generated parity, and same-checkpoint restart evidence
passes for each target. Both use the caller's streaming DataFrame and leave query lifecycle and checkpoint ownership with
the application.

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `transform_with_state(...)` | `GroupedData.transformWithState` | `transform_with_state(key=event.account_id, processor=Counter, output_mode="Update", time_mode="ProcessingTime")` | no | 4.1 only | Status: `design-gated`. Implemented for ordinary PySpark `>=4.1,<4.2`; typed callbacks have one `ValueState`, timer management and callback-scoped time values; opaque native processors retain additional Spark state features. PySpark 4.1 requires pandas, PyArrow, and Protobuf on the driver and workers. The 4.1 image pins Protobuf 6.33.0 and its test session uses RocksDB for Spark's state protocol and multiple column families. PySpark 4.0's Python API has no row-based entry point. The initial 4.1 lane runs V11 tests only. See the [row processor plan](../dev/planning/P10042603.V11-transform-with-state.plan.md). |
| `transform_with_state_in_pandas(...)` | `GroupedData.transformWithStateInPandas` | `transform_with_state_in_pandas(key=event.account_id, processor=Counter, output_mode="Update", time_mode="ProcessingTime")` | no | 4.0 and 4.1 | Status: `design-gated`. Implemented for ordinary PySpark `>=4.0,<4.1` and `>=4.1,<4.2`; uses Pandas batches and requires pandas, PyArrow, and protobuf on the driver and workers. See the [Pandas processor plan](../dev/planning/P10042604.V11-transform-with-state-in-pandas.plan.md). |
| `apply_in_pandas_with_state(...)` | `GroupedData.applyInPandasWithState` | `apply_in_pandas_with_state(key=event.account_id, processor=Counter, output_mode="Update", timeout="none")` | 3.5 | 4.0 and 4.1 | Status: `design-gated`. Separate legacy typed/native compiler operation is implemented for ordinary PySpark `>=3.5,<4.2`; profile support remains gated pending live online/generated parity and same-checkpoint restart evidence. Spark Connect and 4.2 are unclaimed. |
