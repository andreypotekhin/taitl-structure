# Streaming future

## Deferred Work - SparkStreamingDeferredFeatures.md
Include Deferred Work from SparkStreamingDeferredFeatures.md

Ref: SparkStreamingDeferredFeatures.md
Quote:
````
Deferred Families

### Source Ownership

Generated streaming source ownership means Structure would emit `spark.readStream` code. That requires source
declarations for table, path, format, options, schema handling, fail-on-missing behavior, and environment-specific
credentials.

### Sink Ownership

Generated sink ownership means Structure would emit `writeStream` code and possibly table creation or storage layout
policy. That requires sink declarations for format, table or path target, partitioning, output mode compatibility,
checkpoint location, trigger, error handling, and query naming.

### Query Lifecycle

Triggers, checkpoints, query names, `start()`, `awaitTermination()`, stop behavior, and restart behavior form the
streaming lifecycle. A later design should introduce one coherent job model with idempotent deployment, recovery, and
diagnostics for missing checkpoint configuration.

### Watermarks And State Policy

Watermarks and state policies define how long Spark keeps streaming state and when late data is dropped. Future state
policies need explicit event-time fields, delay thresholds, tie policies, and output mode expectations.

### Stateful Operations

Aggregations, windowed aggregations, selected-row helpers, ranking, lag/lead, rolling metrics, exact/subset dedupe, and
many full rowset joins should be admitted one family at a time with capabilities, diagnostics, explain output, and live
streaming tests.

### Stream-Stream Joins

Stream-stream joins need declared input modes, watermark relationships, event-time constraints, join type limits, and
state retention policy.

### Arbitrary Hooks And Foreach Logic

Certifying arbitrary hook bodies, `foreachBatch`, `foreach`, external side effects, and custom sink code requires a
different safety model.

## Future Design Shape

Future streaming orchestration should be explicit and typed. Transforms describe how rows change. Jobs describe where
streaming rows come from, where they go, and how Spark manages query state.

## Admission Rules

A deferred streaming feature becomes eligible only when it has a public DSL shape or configuration key, backend
capability names, diagnostics, explain output, execution and generated-code parity where relevant, live Spark
Structured Streaming evidence, and public reference documentation.
````

To adopt from above:
- stateful operation families with capability checks, explain output, and live evidence;
- additional stream-stream join shapes with declared state and retention semantics; and
- a safety and idempotence model for arbitrary hooks, `foreachBatch`, `foreach`, external side effects, and custom sinks.

## Deferred Work - SparkStreamingDeferredFeatures.back.md
Include Deferred Work from SparkStreamingDeferredFeatures.back.md

Ref: SparkStreamingDeferredFeatures.back.md
Quote:
````
Permanent Non-Goals

Structure does not own `readStream`, `writeStream`, triggers, checkpoints, output-mode application, query names,
query start/stop, deployment, recovery, `foreachBatch`, `foreach`, custom side-effect sinks, external side effects, or
arbitrary state APIs.

## Transformation Features

More complex stateful transformation features remain deferred until their state semantics are compiler-visible:

- chained windowed/stateful aggregations beyond the admitted single-stage event-time and session-window shapes;
- cross and anti stream joins;
- selected-row helpers such as latest or earliest on streaming inputs;
- ranking, lag/lead, and rolling windows; and
- arbitrary state APIs.

## Future Support Bar

A deferred feature can become supported only when Structure defines public DSL or configuration, backend capabilities,
compile-time diagnostics, explain output showing state assumptions, execution and generated-code behavior where both
apply, live Spark Structured Streaming verification, and troubleshooting guidance.
````

To adopt from above:
- keep lifecycle ownership explicit until a separate product decision establishes operational semantics;
- chained stateful aggregations beyond the admitted single-stage shapes;
- cross and anti stream joins;
- selected-row helpers on streaming inputs;
- ranking, lag/lead, and rolling windows; and
- arbitrary state APIs with declared state, timeout, recovery, and restart semantics.
