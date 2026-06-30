# Generated PySpark

Structure can emit generated PySpark code.

Online execution is the default, so users can run transforms through `StructureSession` without committing
generated files. Generated code remains useful when a team wants reviewable build output, provenance, snapshot
tests, or generated-mode runtime entrypoints.

## Generated Class Shape

Each source transform class maps to one generated class.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only if hooks exist

    def run(self, *, orders: DataFrame, customers: DataFrame) -> TransformResult:
        ...
```

A convenience function may also be generated.

```python
def enrich_orders(*, orders, customers, spark, ctx=None):
    return EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
        orders=orders,
        customers=customers,
    ).enriched
```

## Hook-Free Generated Code

If a transform has no hooks, generated code should not import the source transform class.

```python
class NormalizeOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx

    def run(self, *, orders: DataFrame) -> TransformResult:
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")

        orders = orders.select(
            F.col("id").alias("id"),
            F.col("total").cast("decimal(12,2)").alias("total"),
        )

        assert_schema(orders, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")
        return TransformResult({"normalized": orders}, single=True, schema={"normalized": ORDER_NORMALIZED_SCHEMA})
```

## Generated Code Rules

Generated code should be explicit and Spark-visible. It should:

- use `DataFrame` and `Column` operations
- use stable lane names such as `orders` and `published`, plus `spark` and `ctx`
- validate inputs
- validate intermediate schemas by default
- validate outputs
- call hooks only where hooks exist
- pass original named inputs to hooks only when `pass_inputs=True`
- avoid UDFs in compiled paths
- avoid `collect`, `toPandas`, and `rdd` in compiled paths
- include section comments for source subtransforms

## Generated Schema Constants

Generated schema constants such as `ORDER_ENRICHED_SCHEMA` are ordinary PySpark `StructType` values. They are
supported caller-facing artifacts, not only generated transform internals.

```python
from structure_generated.orders.pyspark.schemas.order import ORDER_ENRICHED_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema, project_schema

result = EnrichOrdersGenerated(spark=spark).run(orders=orders, customers=customers)
df = result.enriched
assert_schema(df, result.schema.enriched, name="OrderEnriched", mode="strict")
df = project_schema(df, result.schema["enriched"])
df.write.mode("overwrite").parquet(target_path)
```

Generated `*_SCHEMA` constants are shape-only. Future data-quality constraint metadata must be generated
separately unless a later design adds Spark-compatible metadata without changing schema shape semantics.

Online execution exposes equivalent materialized schemas through `result.schema` after `run(session)`. Use
that online surface when generated files are not committed or imported.

## Ownership Rules

Generated PySpark is optional committed build output owned by the Structure compiler.

Developers should:

- commit generated files with the source or configuration changes that produced them when using generated mode
- review generated-code diffs like other build artifacts when generated files are committed
- regenerate files with `structure compile`
- run `structure compile --fail-on-diff` in CI for projects that commit generated files

Developers should not edit generated files by hand. If generated code is wrong, change the Structure source,
configuration, or generator, then regenerate.

## Why Generated Code Is Longer

Structure lets developers maintain compact source while reviewers and operators can inspect explicit PySpark.

Example: `add_promotion()` source on the left, generated PySpark on the right.

![](../res/img/screenshots/add_promotion.screen.jpg)
