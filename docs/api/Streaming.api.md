# Streaming API

Structure supports a conservative, caller-owned Structured Streaming shape. These declarations and helpers classify
compatibility and compile to streaming-safe DataFrame transformations when their documented conditions are met.
Examples abbreviate `order` as `o` and a second streaming relation as `c`.

## Streaming Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `input(..., streaming=True)` | Streaming input | `input(OrderRaw, streaming=True)` |
| `@transform(streaming=True)` | Compatibility enforcement | `@transform(streaming=True)` |
| `StreamingOutputMode` | Structured Streaming output mode | `mode = StreamingOutputMode.APPEND` |

**Details And Differences**

- `streaming=True` declares a streaming input; omitting it (or setting `False`) declares a static input.
- `StreamingOutputMode` is the typed vocabulary used when explain output reports a caller-required output mode.

## Streaming Operations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `watermark(...)` | `withWatermark` | `watermark(o.event_time, delay="10 minutes")` |
| `window(event_time, duration, slide=None, start=None)` | `functions.window` | `window(o.event_time, "10 minutes")` |
| `session_window(event_time, gap)` | `functions.session_window` | `session_window(o.event_time, "5 minutes")` |
| `drop_duplicates(...)` | `dropDuplicates` / `dropDuplicatesWithinWatermark` | `drop_duplicates(o.id)` |
| `drop_duplicates_within_watermark(...)` | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(o.id)` |
| `@special(type="udf")` | scalar PySpark `udf` | `self.normalize(o.id)` |
| `event_time_between(...)` | Stream-stream time-range predicate | `event_time_between(o.at, c.at, upper="1 hour")` |
| `@raw(..., streaming=True)` | Streaming-safe hook | `@raw(streaming=True)` |

**Details And Differences**

- `watermark(...)` is a compiled-step operation with explicit field and delay.
- `window(...)` retains its keyword-only analytical `WindowSpec` form. Its positional event-time form is a grouping key;
  one call cannot mix the two argument families, while separate calls may use either form in the same transform.
- Event-time windows return `Struct[TimeWindow]` with non-null `start` and `end` timestamps. Tumbling and sliding
  aggregates require a preceding watermark on that same event-time field and use caller-owned `append` or `update` mode.
- `session_window(...)` requires a preceding watermark on the same event-time field, a static positive gap, one
  ordinary grouping key in addition to the session key, and caller-owned `append` mode. Dynamic gaps remain deferred.
- `drop_duplicates(...)` remains cross-mode: batch lowers to `dropDuplicates`, while a streaming frame lowers to
  watermark-bounded `dropDuplicatesWithinWatermark`. `drop_duplicates_within_watermark(...)` makes that streaming-only
  choice explicit and requires `streaming=True` plus a preceding watermark.
- Scalar `@special(type="udf")` expressions are admitted as row-local ordinary-PySpark streaming transformations.
  They retain the existing `warn_on_udfs` warning policy and remain unavailable on Spark Connect.
- `event_time_between(...)` supplies the bounded event-time relation required by supported stream-stream joins.
- `streaming=True` declares the hook safe for its stated streaming shape; Structure does not inspect hook code.

## Lifecycle Boundaries

Supported transform shapes include row-local projection/filter (including scalar Python UDFs), stream-static left/inner
joins and `exists(...)` filtering, event-time and session-window aggregation, bounded dedupe, bounded inner
stream-stream joins, and bounded left/right/full outer and semi stream-stream joins. Callers own
`readStream`, `writeStream`, checkpoints, triggers, output-mode application, and query lifecycle. `foreachBatch` and
`foreach` remain unsupported. Use `examples/streams/adoption.py` as the tested caller-owned recipe shape. See
[V4 Caller-Owned Streaming Migration](../dev/specifications/V4CallerOwnedStreamingMigration.md),
[V9 PySpark Streaming API Coverage](../dev/specifications/V9PySparkStreamingApiCoverage.md), and the
[Execution reference](../background/Execution.back.md).
