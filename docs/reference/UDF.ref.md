# UDF Reference

A UDF lets Spark execute a Python callback as part of a Structure transform. It is a deliberate escape hatch because
Spark cannot optimize the Python body. Prefer a typed Structure expression whenever it can express the calculation.
The [UDF API](../api/UDF.api.md) lists declarations; the [background](../background/UDF.back.md) explains execution and
optimization boundaries.

## Scalar Python UDF

Use `@special(type="udf")` for row-local Python logic with no suitable symbolic expression. Declare both the concrete
result type and whether the result may be null:

```python
from structure import *
from structure.plugin.pyspark import *


class Raw(Schema):
    code = string(nullable=True)


class Clean(Schema):
    code = string(nullable=False)


@transform
class Normalize(Transform):
    rows = input(Raw)
    clean = output(Clean)

    @special(type="udf", return_type=string(), nullable=False)
    def normalize(value):
        return value.strip().upper() if value is not None else "UNKNOWN"

    def apply(self, row: Raw) -> Clean:
        return Clean(code=self.normalize(row.code))
```

The callback runs once per row value. Structure does not inspect it or verify that its results satisfy `nullable=False`;
Spark owns callback execution and any worker errors. `DSL-W0403` warns that Spark cannot optimize the callback. The
transform-level `warn_on_udfs=False` option suppresses the warning without changing execution semantics.

## Arrow UDFs

Arrow UDFs are available only for PySpark `>=4.1,<4.2`. Use `function_type="scalar"` (the default) to process arrays
for individual input batches. Declare a concrete return type from `structure.plugin.pyspark.dsl.types`:

```python
from structure import *
from structure.plugin.pyspark.dsl import types


class Amount(Schema):
    value = long(nullable=True)


class Adjusted(Schema):
    value = long(nullable=True)


@transform
class AddOne(Transform):
    rows = input(Amount)
    adjusted = output(Adjusted)

    @special(type="arrow_udf", return_type=types.long())
    def add_one(values):
        import pyarrow as pa
        import pyarrow.compute as pc

        return pc.add(values, pa.scalar(1, type=pa.int64()))

    def apply(self, row: Amount) -> Adjusted:
        return Adjusted(value=self.add_one(row.value))
```

PyArrow imports can live inside the callback so importing and compiling the transform does not require PyArrow. The
driver and workers need PyArrow when the transform executes.

### Scalar iterator

Choose `scalar_iter` when callback logic should receive and yield batches through one iterator:

```python
@special(type="arrow_udf", return_type=types.long(), function_type="scalar_iter")
def increment_batches(batches):
    import pyarrow as pa
    import pyarrow.compute as pc

    return (pc.add(batch, pa.scalar(1, type=pa.int64())) for batch in batches)


# Inside the transform's apply method:
return Adjusted(value=self.increment_batches(row.value))
```

The callback has one iterator parameter. For multiple inputs, each iterator item is a tuple of Arrow arrays. Calls to
this mode pass input expressions positionally.

### Grouped aggregate and analytic window

A grouped callback returns one scalar for each group. For a full transform with schemas and output binding, see the
[grouped aggregate example](Aggregations.ref.md#arrow-grouped-aggregates).

```python
@special(type="arrow_udf", return_type=types.double(), function_type="grouped_agg")
def mean(values):
    import pyarrow.compute as pc

    return pc.mean(values).as_py()


# Inside an apply method after group_by(row.key):
return GroupResult(key=row.key, mean=self.mean(row.value))
```

To preserve each row and add a rolling aggregate, pass a typed window:

```python
rolling = window(
    partition_by=row.key,
    order_by=row.value,
    frame=rows_between(preceding(1), current_row()),
)
return WindowResult(key=row.key, value=row.value, mean=self.mean(row.value, over=rolling))
```

Grouped Arrow metrics cannot be mixed with built-in aggregate metrics in the same grouped query. Group data can occupy
substantial Python worker memory. Use separate grouped steps when a result needs both built-in and custom metrics.

## Call binding and declaration options

Scalar and grouped callbacks accept positional, named, and mixed expression arguments according to their Python
signatures, including defaults and keyword-only parameters. Scalar Arrow callbacks need at least one positional
parameter for native registration. Iterator callbacks have one iterator parameter and positional expression columns.
Grouped callbacks may take no expression inputs; `over=` is reserved for a Structure `WindowSpec` and is only valid for
grouped mode.

Arrow declarations accept `nullable=True` and optional `deterministic: bool`. Omitting determinism preserves Spark's
default; `False` applies Spark's native nondeterministic setting. Nullability and determinism are declarations, not
callback-result validation. Spark controls output conversion, cardinality, null handling, and worker errors.

## Compatibility

| Callback form | Ordinary PySpark | Spark Connect | Streaming |
| --- | --- | --- | --- |
| Scalar `@special(type="udf")` | Public UDF API where capability is admitted | Public batch UDF API where capability is admitted | Ordinary PySpark contract only |
| Arrow scalar | Exact `>=4.1,<4.2`, batch | Exact `>=4.1,<4.2`, batch | Ordinary PySpark; restart evidence passes |
| Arrow scalar iterator | Exact `>=4.1,<4.2`, batch | Exact `>=4.1,<4.2`, batch | Ordinary PySpark; restart evidence passes |
| Arrow grouped aggregate | Exact `>=4.1,<4.2`, batch | Exact `>=4.1,<4.2`, batch | Excluded |
| Arrow analytic window | Exact `>=4.1,<4.2`, batch | Exact `>=4.1,<4.2`, batch | Excluded |

Arrow support is admitted independently for ordinary and Connect variants. The default PySpark profile `>=3.5,<4.2`
does not support Arrow UDF declarations. Connect streaming has no Arrow UDF support claim. Native, Structure online,
and generated parity passes for the supported 4.1 batch modes; ordinary scalar and iterator streaming restart parity
also passes. For exact target selection and evidence, see the [compatibility ledger](../compatibility/Transforms.compat.md)
and [API tracker](../compatibility/APITracker.md).

Pandas UDFs, registered `call_udf`, UDTFs and Arrow UDTFs remain caller-owned. Structure does not silently convert
unsupported expressions to any UDF form.
