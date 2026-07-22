# V4 Caller-Owned Streaming Migration

## Purpose

This specification defines v4 streaming transformation shapes that Structure supports while callers retain ownership of Structured Streaming jobs. A supported transform accepts caller-created streaming DataFrames and returns a streaming DataFrame plan through both online and generated execution. It does not create a source, configure a sink, or start a query.

## Public Source Contract

The feature adds one source helper:

    session_window(event_time, gap)

`event_time` must be a typed timestamp expression. `gap` must be a positive fixed Spark interval string. The result is `Struct[TimeWindow]` with non-null `start` and `end` timestamp fields. The helper is a grouping key, not an analytical `WindowSpec`, and must not accept `partition_by`, `order_by`, a dynamic Column expression, or an arbitrary Python value.

No join API is added. The existing forms have these v4 meanings:

    rowset_join(payments, on=..., how=Join.LEFT | Join.RIGHT | Join.FULL)
    where(exists(on=...))

`rowset_join(...)` is the stream-stream outer-join form. `exists(...)` is the semi-join form for both stream-stream and stream-static shapes. `not_exists(...)` is not part of this v4 streaming slice.

## Session-Window Rules

A session-window aggregate on a streaming current input is compatible only when all of these hold:

- `watermark(event_time, delay=...)` occurs before the aggregate in the same step flow.
- The `session_window(...)` event time is that watermarked
- `gap` is static and positive.
- The aggregate groups by the session window and at least one additional non-session key.
- The required output mode reported by the operation is exactly `StreamingOutputMode.APPEND`.

The compiler classifies a global session window, an unwatermarked session window, a mismatched watermark field, and a dynamic or invalid gap as `batch_only`. The diagnostic names the missing or invalid condition and links to this specification. Batch use remains legal according to ordinary batch aggregation rules.

## Stream-Stream Join Rules

For `Join.LEFT`, `Join.RIGHT`, `Join.FULL`, and `exists(...)`, both relations must be declared `streaming=True`. The join predicate must include an `event_time_between(left_time, right_time, upper=..., lower=...)` term and equality join keys as required by the normal join contract.

The compiler requires watermarks on both input event-time fields used by the bound. This conservative v4 rule gives one cross-target contract even where Spark can execute with fewer watermarks. All these shapes report and require `StreamingOutputMode.APPEND`.

An outer-join diagnostic must explain that unmatched rows can be delayed until watermark progress proves no future match is possible. A semi-join diagnostic must explain that its right-side watermark and time bound let Spark evict unmatched left rows. Missing input declaration, watermark, or bound is an error for a `@transform(streaming=True)` transform and a warning otherwise when checks are enabled.

## Stream-Static Semi Rules

`exists(...)` is compatible when the current/left relation is streaming, the consulted right relation is declared or observed static, and the normal symbolic join predicate is valid. It lowers as a row-filtering left-semi join without state and has no output-mode requirement. The result exposes only current-row fields.

The following shapes are unsupported: static-left/stream-right semi, stream-static anti, right/full/cross stream-static joins, and any attempt to expose static-right fields through the semi predicate.

## Compiler and Runtime Requirements

Add these capability names to the PySpark backend contract:

    streaming.session_window_aggregate
    streaming.stream_static_left_semi_join
    streaming.stream_stream_outer_join
    streaming.stream_stream_left_semi_join

The IR and shared PySpark recipe records must carry session-window metadata, join kind, declared input modes, watermark field/delay facts, event-time bound, compatibility status, and required output modes. Online execution and generated rendering must consume identical recipes. No code path may add `readStream`, `writeStream`, checkpoint, trigger, output-mode application, `start`, `awaitTermination`, `foreach`, or `foreachBatch`.

`structure explain` must identify the operation, both streaming inputs where applicable, watermark field and delay, event-time bound, cardinality, and required output mode. Generated-source guard tests must reject lifecycle calls.

## Verification

Add fixtures and tests for a static-gap session aggregate, left/right/full bounded stream-stream outer joins, bounded stream-stream semi join, and stream-static semi filtering. Each shape needs symbolic, capability, compatibility, generated-source, online/generated parity, and explain coverage.

Add failing tests for a missing or mismatched watermark, undeclared stream input, missing event-time bound, missing session business key, dynamic/invalid session gap, incompatible output mode, unsupported stream-static direction, and chained stateful operation. Add live bounded-query evidence for every supported family on PySpark 3.5.x and 4.0.x; the test harness must stop its query and delete only its own temporary checkpoint.

## Non-Goals

V4 does not admit dynamic session gaps, session merge tuning, chained stateful operations, chained windows, unbounded state, arbitrary state processors, Pandas/RDD boundaries, sorting/limits/analytic windows/selected-row helpers, or streaming lifecycle ownership.

