# Spark Streaming

Structure supports Spark Structured Streaming transformations: callers own the streaming source and sink, then pass
streaming DataFrames into ordinary execution or generated-code execution transforms. Structure returns a DataFrame plan.

See the exhaustive [streaming API table](../api/Streaming.api.md) for supported declarations, parity, and examples.

## Supported Shape

The basic shape is one streaming current input plus optional static lookup inputs:

```python
orders = spark.readStream.table("orders")
customers = spark.read.table("customers")

result = EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
    orders=orders,
    customers=customers,
)

query = result.writeStream.option("checkpointLocation", checkpoint).toTable("orders_enriched")
```

In the caller-owned compatibility shape, Structure owns the transform plan. Your application owns `readStream`,
`writeStream`, checkpoints, triggers, output modes, and query lifecycle.

## Supported Operations

The supported streaming transform surface includes:

- row-local projections;
- row-local filters;
- schema-only validation;
- stream-static left and inner lookup joins;
- stream-static left-semi filtering through `exists(...)`;
- `watermark(...)` as a DataFrame transformation;
- event-time tumbling/sliding aggregations;
- session-window aggregations;
- watermarked grouped aggregations and bounded dedupe;
- bounded inner, left/right/full outer, and left-semi stream-stream joins when both inputs are declared
  `streaming=True`, both sides have watermarks, and the predicate includes `event_time_between(...)`;
- static-side broadcast hints when supported by the PySpark target;
- hooks marked `streaming=True`;
- compatibility and explain reports that classify transforms as `compatible`, `batch_only`, or `unknown`.

A transform can opt into strict enforcement:

```python
@transform(streaming=True)
class EnrichOrders(Transform):
    ...
```

When the marker is present, unknown or incompatible operations are errors.

Execution and generated-code execution both apply a declared watermark. Structure applies a current-input watermark at its
declared transform step and applies a lookup-input watermark before the lookup join. This keeps state and late-data
semantics independent of the selected execution mode.

## Not Included

The caller-owned compatibility slice does not include:

- generated `readStream` or `writeStream` code;
- query start, stop, trigger, checkpoint, or output-mode ownership;
- generated lifecycle, deployment, or recovery code;
- arbitrary state APIs;
- selected-row, ranking, lag/lead, and rolling-window helpers on streaming inputs;
- right, full, cross, non-equi, or disjunctive rowset joins involving streaming inputs;
- Spark actions such as `collect()`, `count()`, `toPandas()`, and `show()`;
- RDD conversion, Pandas conversion, Python UDF fallback, or local row loops.

See [Spark streaming deferred features](SparkStreamingDeferredFeatures.back.md) for the future feature boundary.

## Hooks

Hooks are opaque. Mark a hook `streaming=True` only when it returns a DataFrame and avoids Spark actions,
RDD/Pandas conversion, streaming lifecycle APIs, external side effects, and stateful streaming operations.

```python
@raw(inout=lane(orders) | lane(orders), streaming=True)
def keep_valid(self, *, orders, spark, ctx):
    return orders.where(F.col("id").isNotNull())
```

Structure trusts this marker; it does not prove arbitrary hook internals are safe.

## Diagnostics

`@transform(streaming=True)` is an explicit all-step streaming-capability contract. Streaming input declarations and
composed streaming outputs trigger compatibility analysis without implicitly changing transform options. Streaming
diagnostics should name the operation and explain the fix. Typical fixes are:

- remove the batch-only operation;
- keep the operation in a batch transform;
- make a side input static;
- replace an opaque hook with compiler-visible Structure DSL;
- mark a hook `streaming=True` only after checking its body;
- add explicit `streaming=True`, `watermark(...)`, or `event_time_between(...)` metadata where the transformation
  requires state;
- keep lifecycle, sinks, checkpoints, and query starts in caller-owned Spark code.

## V10 Continuation

V10 may admit additional stateful transformation shapes only with compiler-visible state stages, explicit watermarks and
retention, public generated APIs, online/generated parity, and restart evidence. Side-effect and arbitrary-state work
remains caller-owned or design-gated. The grouped plans are linked from `docs/dev/project-management/V10.md`.
