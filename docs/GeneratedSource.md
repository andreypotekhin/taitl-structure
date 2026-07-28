# Generated PySpark Source

Structure can emit generated PySpark code.

Execution is the default, so users can run transforms through `StructureSession` without committing generated files.
Code generation remains useful when a team wants reviewable build output, provenance, snapshot tests, or
generated-code execution entrypoints.

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

## Generated Code Options

`generated_code_options` changes generated source while preserving the compiled execution plan. The default is an empty
list and keeps the `run(*, inputs)` API shown above.

```toml
generated_code_options = ["mirror_methods", "embed_exprs", "embed_hooks", "embed_udfs"]
generated_code_hard_wrap = 120
```

`mirror_methods` renders one zero-argument generated method for each schema-returning source step. Its generated class
receives every input in its constructor and `run()` uses instance fields for inputs, lanes, and outputs.

```python
result = EnrichOrdersGenerated(
    spark=spark,
    ctx=ctx,
    orders=orders,
    customers=customers,
).run()
```

Create a new mirror instance for each batch. Expression specials expand at their call sites by default. `embed_exprs`
instead emits static generated helpers. `embed_hooks` and `embed_udfs` copy opted-in raw
hook and UDF bodies into generated source. Without them, generated code delegates to the original transform instance.

### Embedded Raw Hooks

`embed_hooks` emits each raw hook as an ordinary generated method after `run(...)` and calls it at the same declared
hook boundary. The generated module does not import its source transform or construct `_impl` solely for such hooks.
The body is copied from a source snapshot when `structure compile` runs, so regenerate after changing a hook.

The copied body remains opaque to Structure: it is not compiled into expressions or optimized. It must be standalone.
Local imports, parameters, local assignments, Python builtins, `self.spark`, and `self.ctx` are supported. Module globals,
closures, `super()`, and other `self` attributes are rejected with `GEN-E0903`. A Python UDF still needs source-backed
implementation unless `embed_udfs` is also selected.

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
- pass each DataFrame explicitly selected by the hook binding
- avoid UDFs in compiled paths, unless user opt-in
- avoid `collect`, `toPandas`, and `rdd` in compiled paths
- include section comments for source step methods

## Generated Schema Constants

Generated schema constants such as `ORDER_ENRICHED_SCHEMA` are ordinary PySpark `StructType` values. They are
supported caller-facing artifacts, not only generated transform internals.

```python
from structure_generated.store.pyspark.schemas.order import ORDER_ENRICHED_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema, project_schema

result = EnrichOrdersGenerated(spark=spark).run(orders=orders, customers=customers)
df = result.enriched
assert_schema(df, result.schema.enriched, name="OrderEnriched", mode="strict")
df = project_schema(df, result.schema["enriched"])
df.write.mode("overwrite").parquet(target_path)
```

Generated `*_SCHEMA` constants are shape-only. Future data-quality constraint metadata must be generated
separately unless a later design adds Spark-compatible metadata without changing schema shape semantics.

Execution exposes equivalent materialized schemas through `result.schema` after `run(session)`. Use that direct
runtime surface when generated files are not committed or imported.

## Ownership Rules

Generated PySpark is optional committed build output owned by the Structure compiler.

Developers should:

- commit generated files with the source or configuration changes that produced them when using generated mode
- review generated-code diffs like other build artifacts when generated files are committed
- regenerate files with `structure compile`
- run `structure compile --fail-on-diff` in CI for projects that commit generated files

Developers should not edit generated files by hand. If generated code is wrong, change the Structure source,
configuration, or generator, then regenerate.

