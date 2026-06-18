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
- pass original named inputs to hooks only when `pass_inputs=True`
- avoid UDFs in compiled paths
- avoid `collect`, `toPandas`, and `rdd` in compiled paths
- include section comments for source subtransforms

## Ownership Rules

Generated PySpark is committed build output owned by the Structure compiler.

Developers should:

- commit generated files with the source or configuration changes that produced them
- review generated-code diffs like other build artifacts
- regenerate files with `structure compile`
- run `structure compile --fail-on-diff` in CI

Developers should not edit generated files by hand. If generated code is wrong, change the Structure source,
configuration, or generator, then regenerate.

## Why Generated Code Is Longer

Structure allows developers to maintain compact source, while reviewers and operators can inspect explicit PySpark.

Example: add_promotion() method (left) translated into PySpark (right)

![](../docs/img/screenshots/add_promotion.screen.jpg)
