# Generated PySpark

Structure's central deliverable is generated PySpark code.

The generated code should be readable enough to review, stable enough to snapshot-test, and explicit enough to use from Airflow or other Spark job entrypoints.

## Generated Class Shape

Each source transform class maps to one generated class.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only if hooks exist

    def run(self, *, orders: DataFrame, customers: DataFrame) -> DataFrame:
        ...
```

A convenience function may also be generated.

```python
def enrich_orders(*, orders, customers, spark, ctx=None):
    return EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
        orders=orders,
        customers=customers,
    )
```

## Hook-Free Generated Code

If a transform has no hooks, generated code should not import the source transform class.

```python
class NormalizeOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx

    def run(self, *, orders: DataFrame) -> DataFrame:
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")

        df = orders.select(
            F.col("id").alias("id"),
            F.col("total").cast("decimal(12,2)").alias("total"),
        )

        assert_schema(df, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")
        return df
```

## Generated Code Rules

Generated code should:

- use `DataFrame` and `Column` operations
- use stable names such as `df`, `spark`, and `ctx`
- validate inputs
- validate intermediate schemas by default
- validate outputs
- call hooks only where hooks exist
- avoid UDFs in compiled paths
- avoid `collect`, `toPandas`, and `rdd` in compiled paths
- include section comments for source subtransforms

## Why Generated Code Is Longer

Structure source code is concise because it expresses intent:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    return OrderNormalized(
        id=order.id,
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

Generated PySpark is longer because it contains executable details:

```python
df = orders.where(
    F.col("id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.col("total").cast("decimal(12,2)").alias("total"),
)
```

This is a strength: developers maintain compact Structure source, while reviewers and operators can inspect explicit PySpark.
