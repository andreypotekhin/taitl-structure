# Spark Streaming Deferred Features

This specification records the Spark Structured Streaming features intentionally left out of the first supported slice.
It is a contract for future work: deferred features must remain explicit unsupported decisions until they receive their
own lifecycle model, diagnostics, tests, and public documentation.

## Deferred Capability Names

The backend capability model must reserve these names as unsupported for the first slice:

```text
streaming.source_generation
streaming.sink_generation
streaming.trigger_policy
streaming.checkpoint_policy
streaming.output_mode
streaming.query_lifecycle
streaming.watermark
streaming.state_policy
streaming.stream_stream_join
streaming.stateful_aggregation
streaming.windowed_aggregation
streaming.stateful_deduplication
streaming.streaming_selected_row
streaming.streaming_ranking_window
streaming.streaming_lag_lead
streaming.streaming_rolling_window
streaming.foreach_batch
streaming.foreach_sink
```

When a transform, job declaration, hook, or generated artifact requires one of these names before it is implemented, the
compiler must fail through a backend capability or streaming compatibility diagnostic. It must not silently fall back
to a hook, SQL string, local collection, RDD conversion, or Python UDF.

## Source Generation

Future source generation must define:

- source kind: table, path, format, or custom catalog reference;
- schema source: declared Structure schema, Spark schema object, or inferred metadata path;
- options and secrets boundary;
- startup diagnostics for missing source configuration;
- generated import and call shape for `spark.readStream`;
- parity expectations for online and generated execution.

The first slice has none of these. Callers own all `readStream` code.

## Sink Generation

Future sink generation must define:

- sink kind: table, path, console, memory, foreach, or custom sink;
- output mode compatibility;
- checkpoint requirement and validation;
- trigger and query naming policy;
- partitioning and storage layout policy if admitted;
- generated call shape for `writeStream`;
- recovery behavior for missing or incompatible checkpoints.

Generated transform classes must continue to return DataFrames until a streaming job artifact is explicitly designed.

## Query Lifecycle

Future lifecycle ownership must define who calls `start()`, who blocks or returns a query handle, how stop behavior is
managed, and how failed query startup is diagnosed. A partial lifecycle model is not allowed; source, sink, trigger,
checkpoint, output mode, and query naming policy must be coherent before Structure generates query-starting code.

## Watermarks And State Policy

Future watermarks and state policies must define:

- event-time field declaration;
- allowed delay thresholds;
- late-data behavior;
- state retention expectations;
- output mode compatibility;
- diagnostics when a stateful operation lacks a watermark or state policy;
- explain output showing state assumptions.

Watermark policy must be source and operation aware. A transform-local default is not enough for stream-stream joins or
windowed aggregations.

## Stateful Operations

Stateful streaming support must be admitted by operation family:

- grouped and windowed aggregations;
- selected-row helpers such as latest/earliest;
- exact and subset dedupe;
- ranking, lag/lead, and rolling windows where Spark admits them;
- stream-stream joins.

Each family must define accepted output modes, required watermarks, state growth risks, deterministic tie policy, and
live streaming tests. Until then, the operation remains `batch_only` for streaming compatibility.

## Hooks And Foreach

`streaming_safe=True` only admits ordinary DataFrame-returning hooks inside the first slice. Future `foreachBatch`,
`foreach`, external side effects, and custom sink hooks need a separate lifecycle and idempotence contract. They must
make retry behavior, side effects, and failure handling explicit.

## Acceptance For Future Admission

A deferred feature can move into a sprint only when its plan includes:

- design, implementation specification, and public reference updates;
- backend capability support and unsupported cases;
- diagnostics with links and concrete user actions;
- explain output;
- generated-source snapshots;
- online/generated parity where relevant;
- live Spark Structured Streaming evidence;
- troubleshooting entries for likely operational failures.
