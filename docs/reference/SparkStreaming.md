# Spark Streaming

Structure supports a narrow Spark Structured Streaming first slice: callers own the streaming source and sink, then pass
streaming DataFrames into ordinary online or generated Structure transforms. Structure returns a DataFrame plan. It does
not start streaming queries.

## Supported Shape

The supported shape is one streaming current input plus optional static lookup inputs:

```python
orders = spark.readStream.table("orders")
customers = spark.read.table("customers")

result = EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
    orders=orders,
    customers=customers,
)

query = result.writeStream.option("checkpointLocation", checkpoint).toTable("orders_enriched")
```

Structure owns the transform plan. Your application owns `readStream`, `writeStream`, checkpoints, triggers, output
modes, and query lifecycle.

## Supported Operations

The first slice supports:

- row-local projections;
- row-local filters;
- schema-only validation;
- stream-static left and inner lookup joins;
- static-side broadcast hints when supported by the PySpark target;
- hooks marked `streaming_safe=True`;
- compatibility and explain reports that classify transforms as `compatible`, `batch_only`, or `unknown`.

A transform can opt into strict enforcement:

```python
@transform(streaming_compatible=True)
class EnrichOrders(Transform):
    ...
```

When the marker is present, unknown or incompatible operations are errors.

## Not Included

The first slice does not include:

- generated `readStream` or `writeStream` code;
- query start, stop, trigger, checkpoint, or output-mode ownership;
- watermarks and state policies;
- stream-stream joins;
- streaming aggregations, windowed aggregations, or stateful dedupe;
- right, full, cross, non-equi, or disjunctive rowset joins involving streaming inputs;
- Spark actions such as `collect()`, `count()`, `toPandas()`, and `show()`;
- RDD conversion, Pandas conversion, Python UDF fallback, or local row loops.

See [Spark streaming deferred features](SparkStreamingDeferredFeatures.md) for the future feature boundary.

## Hooks

Hooks are opaque. Mark a hook `streaming_safe=True` only when it returns a DataFrame and avoids Spark actions,
RDD/Pandas conversion, streaming lifecycle APIs, external side effects, and stateful streaming operations.

```python
@after(normalize, lane=orders, streaming_safe=True)
def keep_valid(self, *, orders, spark, ctx):
    return orders.where(F.col("id").isNotNull())
```

Structure trusts this marker; it does not prove arbitrary hook internals are safe.

## Diagnostics

Streaming diagnostics should name the operation and explain the fix. Typical fixes are:

- remove the batch-only operation;
- keep the operation in a batch transform;
- make a side input static;
- replace an opaque hook with compiler-visible Structure DSL;
- mark a hook `streaming_safe=True` only after checking its body;
- wait for a later streaming orchestration feature when the transform needs state, watermarks, or generated sinks.
