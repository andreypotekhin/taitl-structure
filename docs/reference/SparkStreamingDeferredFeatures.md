# Spark Streaming Non-Goals And Deferred Transform Features

Structure supports streaming DataFrame transformations without owning streaming query lifecycle. Some Spark streaming
features are permanent non-goals; others are transformation features admitted only with explicit state policy,
diagnostics, tests, and documentation.

## Permanent Non-Goals

Structure does not generate streaming sources:

```python
spark.readStream...
```

Structure does not generate streaming sinks or start queries:

```python
df.writeStream...
query.start()
```

Structure also does not own:

- triggers;
- checkpoint locations;
- query names and query lifecycle;
- selected-row helpers such as latest or earliest on streaming inputs;
- `foreachBatch`, `foreach`, custom sinks, and external side effects.

## Transformation Features

Structure supports transform-scoped `watermark(...)`, watermarked aggregations, watermarked dedupe, and inner
stream-stream joins with `event_time_between(...)` when the relevant inputs are declared streaming.

More complex stateful transformation features remain deferred until their state semantics are compiler-visible:

- windowed aggregations beyond the admitted aggregate shape;
- outer and semi stream-stream joins;
- selected-row helpers such as latest or earliest on streaming inputs;
- ranking, lag/lead, and rolling windows;
- arbitrary state APIs.

Structure reports output-mode requirements, but the caller applies them in `writeStream` code.

## Future Support Bar

A deferred feature can become supported only when Structure defines:

- public DSL or configuration;
- backend capabilities;
- compile-time diagnostics with clear fixes;
- explain output showing state assumptions;
- online and generated behavior where both apply;
- live Spark Structured Streaming verification;
- troubleshooting guidance for likely operational failures.

Until then, keep these concerns in caller-owned Spark code and pass compatible streaming DataFrames into Structure
transforms.
