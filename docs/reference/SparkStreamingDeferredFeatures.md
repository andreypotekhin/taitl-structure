# Spark Streaming Deferred Features

These Spark Structured Streaming features are intentionally outside Structure's first streaming slice. They may be
valid Spark patterns, but Structure does not support them until they have explicit lifecycle policy, diagnostics, tests,
and documentation.

## Deferred Features

Structure does not yet generate streaming sources:

```python
spark.readStream...
```

Structure does not yet generate streaming sinks or start queries:

```python
df.writeStream...
query.start()
```

Structure also does not yet own:

- triggers;
- checkpoint locations;
- output modes;
- query names and query lifecycle;
- watermarks;
- state retention policy;
- stream-stream joins;
- streaming aggregations and windowed aggregations;
- stateful dedupe;
- selected-row helpers such as latest or earliest on streaming inputs;
- `foreachBatch`, `foreach`, custom sinks, and external side effects.

## Why They Are Deferred

These features are operational contracts, not just transform syntax. A streaming aggregation, for example, may need an
event-time field, a watermark delay, an output mode, a checkpoint, and a state-retention policy. Structure should not
guess those values or hide them in generated code.

## Future Support Bar

A deferred feature can become supported only when Structure defines:

- public DSL or configuration;
- backend capabilities;
- compile-time diagnostics with clear fixes;
- explain output showing lifecycle and state assumptions;
- online and generated behavior where both apply;
- live Spark Structured Streaming verification;
- troubleshooting guidance for likely operational failures.

Until then, keep these concerns in caller-owned Spark code and pass compatible streaming DataFrames into Structure
transforms.
