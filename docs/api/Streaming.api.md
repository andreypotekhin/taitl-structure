# Streaming API

Structure supports a conservative, caller-owned Structured Streaming shape. These declarations and helpers classify
compatibility and compile to streaming-safe DataFrame transformations when their documented conditions are met.
Examples abbreviate `order` as `o` and a second streaming relation as `c`.

## Streaming Declarations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `input(..., streaming=StreamingMode.YES)` | Streaming input | `input(OrderRaw, streaming=StreamingMode.YES)` |
| `StreamingMode` | Streaming input mode | `input(OrderRaw, streaming=StreamingMode.YES)` |
| `@transform(streaming_compatible=True)` | Compatibility enforcement | `@transform(streaming_compatible=True)` |
| `StreamingOutputMode` | Structured Streaming output mode | `mode = StreamingOutputMode.APPEND` |

**Details And Differences**

- `StreamingMode` declares the nature of an input; strict transform compatibility rejects unknown or invalid shapes.
- `StreamingOutputMode` is the typed vocabulary used when explain output reports a caller-required output mode.

## Streaming Operations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `watermark(...)` | `withWatermark` | `watermark(o.event_time, delay="10 minutes")` |
| `window(event_time, duration, slide=None, start=None)` | `functions.window` | `window(o.event_time, "10 minutes")` |
| `drop_duplicates(...)` | `dropDuplicates` / `dropDuplicatesWithinWatermark` | `drop_duplicates(o.id)` |
| `drop_duplicates_within_watermark(...)` | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(o.id)` |
| `@special(type="udf")` | scalar PySpark `udf` | `self.normalize(o.id)` |
| `event_time_between(...)` | Stream-stream time-range predicate | `event_time_between(o.at, c.at, upper="1 hour")` |
| `@raw(..., streaming_safe=True)` | Streaming-safe hook | `@raw(streaming_safe=True)` |

**Details And Differences**

- `watermark(...)` is a compiled-step operation with explicit field and delay.
- `window(...)` retains its keyword-only analytical `WindowSpec` form. Its positional event-time form is a grouping key;
  one call cannot mix the two argument families, while separate calls may use either form in the same transform.
- Event-time windows return `Struct[TimeWindow]` with non-null `start` and `end` timestamps. Tumbling and sliding
  aggregates require a preceding watermark on that same event-time field and use caller-owned `append` or `update` mode.
- `drop_duplicates(...)` remains cross-mode: batch lowers to `dropDuplicates`, while a streaming frame lowers to
  watermark-bounded `dropDuplicatesWithinWatermark`. `drop_duplicates_within_watermark(...)` makes that streaming-only
  choice explicit and requires `StreamingMode.YES` plus a preceding watermark.
- Scalar `@special(type="udf")` expressions are admitted as row-local ordinary-PySpark streaming transformations.
  They retain the existing `warn_on_udfs` warning policy and remain unavailable on Spark Connect.
- `event_time_between(...)` supplies the bounded event-time relation required by supported stream-stream joins.
- `streaming_safe=True` declares the hook safe for its stated streaming shape; Structure does not inspect hook code.

## Lifecycle Boundaries

Supported transform shapes include row-local projection/filter (including scalar Python UDFs), stream-static joins,
event-time window aggregation and bounded dedupe,
and bounded inner stream-stream joins. Callers own `readStream`, `writeStream`, checkpoints, triggers, output modes,
and query lifecycle. `foreachBatch` and `foreach` remain unsupported. See the
[Execution reference](../background/Execution.back.md).
