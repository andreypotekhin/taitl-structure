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
  a watermarked first event-time window aggregate, stateless work, then a second
  `window(window_time(first_window), ...)`
  aggregate. The generated transform uses public `functions.window_time`; broader chained stateful operations remain
  rejected with `STREAM-E0801`.
- `session_window(...)` requires a preceding watermark on the same event-time field, a static positive gap, one
  ordinary grouping key in addition to the session key, and caller-owned `append` mode. Dynamic gaps remain deferred.
- Broad analytic windows and global selected-row helpers are streaming-ineligible. For finite event-time selection,
  use grouped `first_value(...)` or `last_value(...)` inside a watermarked `window(...)`; these preserve the typed
  selected value and lower through public `min_by(...)`/`max_by(...)`. The existing `latest_by(...)` and
  `earliest_by(...)` relation helpers remain batch-only.
- `drop_duplicates(...)` remains cross-mode: batch lowers to `dropDuplicates`, while a streaming frame lowers to
  watermark-bounded `dropDuplicatesWithinWatermark`. `drop_duplicates_within_watermark(...)` makes that streaming-only
  choice explicit and requires `streaming=True` plus a preceding watermark.
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
state assumptions visible in explain output; it does not make Structure own query lifecycle or recovery.

- The supported composition boundary is one admitted stateful operation followed by stateless work. A second stateful
  operation remains rejected with `STREAM-E0801` unless a specific finite contract is admitted.
- Cross and anti stream-stream joins remain rejected until finite completion, retention, and restart behavior are
  proven.
- Arbitrary state APIs remain design-gated. A future admission requires typed input, state, and output Schemas;
  grouping keys; event-time or processing-time timeout policy; initialization, update, and removal behavior; a
  resolved PySpark profile; a visible generated-code or hook boundary; and checkpoint/restart evidence. See the
  [arbitrary-state contract](../dev/specifications/V9StreamingDesignGatedFeatures.md#arbitrary-state-apis).
- Pandas, RDD, `mapInPandas`, and state-processor boundaries remain unsupported because their execution and state
  semantics are opaque to the compiler.

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

## Caller-Owned Side-Effect Safety

Before starting a `foreachBatch` sink, callers provide a `ForeachBatchSafety` declaration with a stable `sink_identity`,
an `idempotence_key` such as `snapshot_id:batch_id`, a `retry_policy` (`at_least_once`, `idempotent`, or
`transactional`), and a stable `snapshot_id`. The adoption helper rejects missing or unknown declarations before
calling `start()`. These declarations make the recovery assumptions reviewable; they do not make callback code
idempotent, transactional, or secure. The callback and its sink remain the caller's responsibility, including using the
declared key, handling retries, and ensuring that the checkpoint and snapshot identity remain compatible.

## Typed Arbitrary-State Contract

Arbitrary state remains design-gated; `ArbitraryStateContract` is a metadata completeness guard, not a state processor
runtime. Before caller-owned `applyInPandasWithState`, `transformWithState`, or a related state API is reviewed, the
contract records typed input, key, state, and output Schemas; grouping fields; timeout policy, clock, and duration;
initialization, update, and removal behavior; target PySpark profile; hook boundary; checkpoint identity; serialized
state version; and restart policy. `contract.validate()` rejects missing or inconsistent declarations with
`ARBITRARY-STATE-E0901`, `ARBITRARY-STATE-E0902`, or `ARBITRARY-STATE-E0903`.

Validation does not start a query, generate a state processor, own a checkpoint, or prove recovery. The caller still
owns the native PySpark API and live restart evidence. Structure must not promote the streaming ledger row until a
separate runtime contract and PySpark 3.5/4.0 evidence exist.

## SearchDocuments Caller-Owned Run Handoff

The SearchDocuments streaming path is design-gated and does not start a query for the caller. Before a caller adopts a
future ready-to-start path, `SearchDocumentsRunContract` records one immutable serving run:

- `snapshot_id` binds the index, score cache, feedback, popularity, and policy snapshots;
- `checkpoint_identity`, `sink_identity`, `trigger`, and `completion_window` identify the operational run;
- output mode is `append` and event time is immutable `requested_at`;
- `refresh_restart_policy` requires a new run whenever a serving snapshot changes;
- `finality_policy` requires one final result set with no later revisions;
- `downstream_materialization` states where final results become durable before serving.

`SearchDocumentsRunContract.validate()` checks this handoff metadata only when the Search streaming proving switch is
enabled. It is currently disabled for integration delivery, so validation is intentionally inactive. It does not prove
bounded top-K state, watermark completion, stream-stream join support, checkpoint recovery, or generated-code readiness.
Those remain separate compiler and live-evidence gates.

`completion_window` must be a positive finite Spark duration such as `10 minutes`; an unbounded or zero-width
declaration cannot establish when a query is final.

## SearchDocuments Finite-Window Top-K Contract

`SearchFiniteTopKContract` records the state boundary required for the two bounded selection stages without admitting a
runtime implementation. `candidate_admission` must retain exactly 1,000 rows per query window and `overlap_narrowing`
must retain exactly 100. Both stages require a `query_id` grouping key, `requested_at` event time, compatible watermark
and completion-window declarations, append output, the immutable serving `snapshot_id`, and restart on the same
checkpoint only when that snapshot is unchanged.

The order contract is `score desc, document_id asc`; the identifier tie-breaker is mandatory. The metadata guard is
inactive while `SEARCH_STREAMING_CONTRACTS_ENABLED` is false; when enabled, it still does not lower `row_number`,
provide arbitrary state, or prove live restart behavior. Until those runtime and evidence gates pass,
SearchDocuments remains design-gated and callers must use a batch/materialization boundary.

`watermark_delay` and `completion_window` must be positive finite durations, and `grouping_key` must include `query_id`.
