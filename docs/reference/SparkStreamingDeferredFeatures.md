# Spark Streaming Deferred Features

Structure supports streaming DataFrame transformations today without owning streaming query lifecycle. v3 promotes the
source, sink, and lifecycle families into planned orchestration work; custom side-effect sinks and arbitrary opaque
state remain non-goals.

## Planned v3 Lifecycle Ownership

Structure-owned streaming orchestration means generated streaming source declarations:

```python
spark.readStream...
```

It also means generated streaming sink and query setup:

```python
df.writeStream...
query.start()
```

The v3 lifecycle contract must make these policies explicit:

- triggers;
- checkpoint locations;
- query names and query lifecycle;
- output modes;
- deployment and recovery evidence.

## Permanent Non-Goals

Structure still does not own `foreachBatch`, `foreach`, custom side-effect sinks, external side effects, or arbitrary
state APIs.

## Transformation Features

Structure supports transform-scoped `watermark(...)`, watermarked aggregations, watermarked dedupe, and inner
stream-stream joins with `event_time_between(...)` when the relevant inputs are declared streaming.

More complex stateful transformation features remain deferred until their state semantics are compiler-visible:

- windowed aggregations beyond the admitted aggregate shape;
- outer and semi stream-stream joins;
- selected-row helpers such as latest or earliest on streaming inputs;
- ranking, lag/lead, and rolling windows;
- arbitrary state APIs.

The caller-owned compatibility slice reports output-mode requirements, but the caller applies them in `writeStream`
code. v3 streaming orchestration moves admitted output-mode policy into generated lifecycle code.

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
