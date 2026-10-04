# API Reference

This describes Structure's public, compiler-visible API.

If you are just starting using this library, see [QuickRef.md](../QuickRef.md) for an introduction.

`supported` means the public contract is available now. `planned` needs a more complete type, cardinality, or
determinism contract. `deferred` and `unsupported` deliberately stay outside the current scope.
Structure is not a one-to-one PySpark
wrapper: admitted APIs remain typed, symbolic, capability-checked, explainable, and readable in generated code.

The default target is ordinary PySpark `>=3.5,<4.1`; completed compiler-visible batch features also target Spark
Connect. See [Compatibility.md](../Compatibility.md) for the full target policy,
[API.md](../API.md#api-documentation-map) for the API documentation gateway, and [APICompatibility.md](../compatibility/APICompatibility.md)
for the family-level compatibility summary.

The examples use declaration forms from the [Schema reference](Schema.ref.md) and the
[Transform reference](Transform.ref.md).

## Core APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Schemas | supported | `StructType`, SQL types | [Schema reference](Schema.ref.md) |
| Transforms and hooks | supported | DataFrame pipeline | [Transform reference](Transform.ref.md) |
| Expressions | supported | Column and SQL-function subset | [Expressions API](../api/Expressions.api.md) |
| Geospatial | target-gated | Native PySpark 4.1+ and provider namespaces | [Geospatial reference](Geospatial.ref.md) |

**Details And Differences**

- Schema classes own field names, aliases, types, and nullability instead of exposing raw Spark schema objects.
- `timestamp()` and `timestamp_ntz()` remain distinct. `convert_timezone(...)` takes an NTZ source and typed String
  zone expressions; see the [expression reference](../api/Expressions.api.md#sql-function-helpers) for its signature.
- `to_timestamp_ntz(...)` also returns the distinct NTZ type and accepts a typed, row-dependent String format.
- Generic timestamp helpers follow `spark.sql.timestampType`, while `_ltz` and `_ntz` helpers have fixed types.
  `Temporal` and `Interval` are string constants for `extract(...)` and exact interval construction; see the
  [expression API](../api/Expressions.api.md#sql-function-helpers) and [Schema API](../api/Schemas.api.md).
- Transform source is compiler-visible. `@raw` remains the explicit boundary for caller-supplied PySpark behavior.
- Expression truthiness, raw SQL strings, UDTFs, and arbitrary callback bodies are unsupported. Scalar
  `@special(type="udf")` remains an ordinary-PySpark row-local feature with its warning policy.

```python
from structure import *
from structure.plugin.pyspark import *


class PublishedOrder(Schema):
    id = string(nullable=False)


class Publish(Transform):
    orders = input(Order)
    published = output(PublishedOrder)

    def publish(self, order: Order) -> PublishedOrder:
        where(order.id.is_not_null())
        return PublishedOrder.project(order)(id=order.id)
```

The schema, transform, and expression remain visible to compile-time checking and generated output.

## Analytical APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Joins | supported | DataFrame joins and windowed matching | [Join reference](Join.ref.md) |
| Aggregations and dedupe | supported | GroupedData and windows | [Aggregations reference](Aggregations.ref.md) |
| Sketches and bitmaps | supported | Typed opaque HLL/Bitmap state | [Aggregations reference](Aggregations.ref.md#opaque-sketch-metrics) |
| Inline and reusable windows | supported | `Window` and window functions | [Windows API](../api/Windows.api.md) |
| Array/map helpers | supported | Higher-order and map SQL functions | [Collections API](../api/Collections.api.md) |
| Relation operations | supported | Sets, order, assertions, hierarchy, sampling | [API](../api/Relations.api.md) |

**Details And Differences**

- Cross joins need explicit `allow_cartesian=True`; right/full join projections must handle nullable sides.
- Aggregates use typed helpers rather than dictionary/list aggregate syntax. Ordered selection is explicit.
- Array and map callbacks return symbolic expressions; they do not run Python code for every row.

```python
left_join(on=(order.tenant_id == customer.tenant_id) & (order.customer_id == customer.id))
group_by(tenant_id=order.tenant_id)
return CustomerTotal(order_count=count(), total=sum(order.total))
```

Choose the join cardinality and aggregate grain explicitly; the corresponding focused references contain the full
operation examples and edge conditions.

## Runtime And Streaming APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| PySpark batch | supported | Spark DataFrames | [Execution reference](Execution.ref.md) |
| Spark Connect batch | supported | Spark Connect DataFrame and Column APIs | [Compatibility.md](../Compatibility.md) |
| Streaming transforms | supported | Streaming-safe shapes | [Streaming API](../api/Streaming.api.md) |
| Generated lifecycle | unsupported | `readStream`, `writeStream` | [Streaming](../background/Streaming.back.md) |

**Details And Differences**

- Callers own streaming sources, sinks, triggers, checkpoints, output modes, and query lifecycle. Event-time
  tumbling/sliding aggregation, session-window aggregation, watermark-bounded dedupe, bounded stream-stream joins,
  stream-static joins, and scalar Python UDFs are compiler-visible transformations; scalar UDFs are batch-supported
  on Spark Connect but remain ordinary-PySpark-only for streaming. Use the tested application-controlled recipe in
  [`examples/streams/adoption.py`](../../examples/streams/adoption.py) for source/sink/query lifecycle code.
- Classic-only Spark internals such as SparkContext, RDDs, JVM access, and `_jdf` are unsupported for Spark Connect.

```python
events = spark.readStream.schema(event_schema).json(input_path)
result = WindowedOrders(events=events).run(session)
query = result.totals.writeStream.outputMode("append").option("checkpointLocation", checkpoint).start(output_path)
```

The caller controls the source, sink, checkpoint, trigger, and query lifecycle. The transform describes only the
admitted
typed operation graph.

## Planned And Unsupported Surface

The [API compatibility overview](../compatibility/APICompatibility.md) classifies the current PySpark transformation
baseline, and the per-family compatibility ledgers classify function and Structured Streaming surfaces. The
rows below remain a compact orientation aid. Loading, storage, actions, and orchestration are not transformation APIs
and stay outside Structure's scope.

| API Area | Status | PySpark Parity | Details |
| --- | --- | --- | --- |
| Struct mutation | supported | `withField`, `dropFields` | Explicit result Schema preserves nested type and aliases. |
| Bitwise expressions | supported | Bitwise functions | Integer/long-only typed expressions. |
| Nearest as-of; stats | supported | Advanced joins | Typed statistics with explicit ties and targets. |
| Join reordering | design-gated | Cost-based join planning | No public helper; planning must remain explainable. |
| Array variants; generators | partial | `slice`/`explode`/`inline` | Typed only; raw needs contracts. |
| Window order; more aggregates | supported | Window and aggregate frames | Typed window and aggregate helpers. |
| Collection basics | supported | Core arrays/maps | [Collections API](../api/Collections.api.md) |
| Raw APIs/lifecycle | unsupported | `expr`, `WindowSpec`, UDTF | Use hooks; caller controls lifecycle. |

For detailed restrictions, diagnostics, and feature-admission rationale, consult [APICompatibility.md](../compatibility/APICompatibility.md),
[APITracker.md](../compatibility/APITracker.md), and the linked reference pages.

## Next Steps

Get started: [GettingStarted.md](../GettingStarted.md)

Reference docs: [Reference.md](../Reference.md)
