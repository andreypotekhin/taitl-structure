# Spark Streaming Deferred Features

Structure supports compiler-visible streaming DataFrame transformations without owning streaming query lifecycle.
Sources, sinks, and lifecycle remain caller-owned; custom side-effect sinks and arbitrary opaque state remain non-goals.

See the exhaustive [streaming API table](../api/Streaming.api.md) for the supported transformation surface.

## Permanent Non-Goals

Structure does not own `readStream`, `writeStream`, triggers, checkpoints, output-mode application, query names,
query start/stop, deployment, recovery, `foreachBatch`, `foreach`, custom side-effect sinks, external side effects, or
arbitrary state APIs.

## Transformation Features

Structure supports transform-scoped `watermark(...)`, event-time and session-window aggregations, watermarked dedupe,
stream-static joins and left-semi filtering, and admitted bounded stream-stream joins with `event_time_between(...)`
when the relevant inputs are declared streaming.

More complex stateful transformation features remain deferred until their state semantics are compiler-visible:

- chained windowed/stateful aggregations beyond the admitted single-stage event-time and session-window shapes;
- cross and anti stream joins;
- selected-row helpers such as latest or earliest on streaming inputs;
- ranking, lag/lead, and rolling windows;
- arbitrary state APIs.

The compatibility slice reports output-mode requirements, but the caller applies them in `writeStream` code.

## Future Support Bar

A deferred feature can become supported only when Structure defines:

- public DSL or configuration;
- backend capabilities;
- compile-time diagnostics with clear fixes;
- explain output showing state assumptions;
- execution and generated-code execution behavior where both apply;
- live Spark Structured Streaming verification;
- troubleshooting guidance for likely operational failures.

Keep lifecycle concerns in caller-owned Spark code and pass compatible streaming DataFrames into Structure transforms.
