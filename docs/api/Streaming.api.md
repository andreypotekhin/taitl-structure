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
| `@special(type="udf")` | scalar PySpark `udf` | `self.normalize(o.id)` |
| `event_time_between(...)` | Stream-stream time-range predicate | `event_time_between(o.at, c.at, upper="1 hour")` |
| `@raw(..., streaming=True)` | Streaming-safe hook | `@raw(streaming=True)` |

**Details And Differences**

- `watermark(...)` is a compiled-step operation with explicit field and delay.
- `window(...)` retains its keyword-only analytical `WindowSpec` form. Its positional event-time form is a grouping key;
  one call cannot mix the two argument families, while separate calls may use either form in the same transform.
- Event-time windows return `Struct[TimeWindow]` with non-null `start` and `end` timestamps. Tumbling and sliding
  aggregates require a preceding watermark on that same event-time field and use caller-owned `append` or `update` mode.
- `window_time(...)` accepts only a `TimeWindow` produced by `window(...)` and is supported for one chained pair:
  a watermarked first event-time window aggregate, stateless work, then a second `window(window_time(first_window), ...)`
  aggregate. The generated transform uses public `functions.window_time`; broader chained stateful operations remain
  rejected with `STREAM-E0801`.
- `session_window(...)` requires a preceding watermark on the same event-time field, a static positive gap, one
  ordinary grouping key in addition to the session key, and caller-owned `append` mode. Dynamic gaps remain deferred.
- `drop_duplicates(...)` remains cross-mode: batch lowers to `dropDuplicates`, while a streaming frame lowers to
  watermark-bounded `dropDuplicatesWithinWatermark`. `drop_duplicates_within_watermark(...)` makes that streaming-only
  choice explicit and requires `streaming=True` plus a preceding watermark.
- Scalar `@special(type="udf")` expressions are admitted as row-local ordinary-PySpark streaming transformations.
  They retain the existing `warn_on_udfs` warning policy and remain unavailable on Spark Connect.
- Variant fields and helpers are admitted as profile-gated streaming transformations on ordinary PySpark 4 profiles.
  PySpark 4.0 live evidence covers parsing, extraction, schema inspection, object conversion, JSON-null testing,
  watermarked `schema_of_variant_agg`, and typed inner/outer TVF expansion; PySpark 3.5 fails through the standard
  capability diagnostic before execution. PySpark 4.2-only helpers such as `is_valid_variant(...)` remain
  capability-gated until a 4.2 live lane exists.
- `event_time_between(...)` supplies the bounded event-time relation required by supported stream-stream joins.
- `streaming=True` declares the hook safe for its stated streaming shape; Structure does not inspect hook code.

## Lifecycle Boundaries

Supported transform shapes include row-local projection/filter (including scalar Python UDFs), stream-static left/inner
joins and `exists(...)` filtering, event-time and session-window aggregation, bounded dedupe, bounded inner
stream-stream joins, and bounded left/right/full outer and semi stream-stream joins. Callers own
`readStream`, `writeStream`, checkpoints, triggers, output-mode application, query lifecycle, and side effects.
`foreachBatch` is caller-owned-guided through `examples.streams.adoption.start_foreach_batch_query(...)`; generated
Structure modules must not contain `foreachBatch`. Row-level `foreach` remains design-gated until a side-effect
contract defines sink identity, idempotence, retry, and recovery behavior. Use `examples/streams/adoption.py` as the
tested caller-owned recipe shape. See
[Spark Streaming](../dev/specifications/SparkStreaming.md), and the
[Execution reference](../background/Execution.back.md).
