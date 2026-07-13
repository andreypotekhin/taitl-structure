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
- `StreamingOutputMode` is the typed output-mode vocabulary for lifecycle configuration; generated sinks are planned.

## Streaming Operations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `watermark(...)` | `withWatermark` | `watermark(o.event_time, delay="10 minutes")` |
| `event_time_between(...)` | Stream-stream time-range predicate | `event_time_between(o.at, c.at, upper="1 hour")` |
| `@raw(..., streaming_safe=True)` | Streaming-safe hook | `@raw(streaming_safe=True)` |

**Details And Differences**

- `watermark(...)` is a compiled-step operation with explicit field and delay.
- `event_time_between(...)` supplies the bounded event-time relation required by supported stream-stream joins.
- `streaming_safe=True` declares the hook safe for its stated streaming shape; Structure does not inspect hook code.

## Lifecycle Boundaries

Supported transform shapes include row-local projection/filter, stream-static joins, watermarked aggregation and dedupe,
and bounded inner stream-stream joins. Callers currently own `readStream`, `writeStream`, checkpoints, triggers,
output modes, and query lifecycle. Generated sources and sinks are planned for Sprint 16; `foreachBatch` and `foreach`
remain unsupported. See [Spark streaming](../reference/SparkStreaming.md) and
[streaming deferred features](../reference/SparkStreamingDeferredFeatures.md).
