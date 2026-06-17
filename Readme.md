# Structure

**Structure** is a Python DSL and code generator for schema-enforced, IDE-friendly data pipelines that compile to clean PySpark DataFrame code.

Structure lets developers describe large dataset processing as typed schema-to-schema transformations while generating Spark optimizer-visible Column and DataFrame expressions.

## Why Structure?

Hand-written PySpark is powerful, but large pipelines often become difficult to maintain:

- Column names are frequently embedded as strings.
- Intermediate schemas are often implicit.
- Schema drift is hard to catch before runtime.
- Business logic can become hidden inside Python UDFs or row-wise callbacks.
- Airflow DAGs can become tightly coupled to transformation internals.
- Generated or repeated transformation code is hard to review consistently.

Structure addresses these problems by making schemas and transformations explicit while generating readable PySpark code that remains visible to Spark's optimizer.

## Less Source Code, More Generated Spark Code

A Structure transform can express projection, filtering, normalization, and serial enrichment joins in compact schema-oriented Python.

### Source Structure code

```python
from pyspark.sql import functions as F

from structure import (
    Transform,
    transform,
    input,
    expr_fn,
    where,
    after,
    lower,
    trim,
    to_decimal,
    Join,
    JoinHint,
    SchemaMode,
)

from pipeline_src.schemas.order import (
    OrderRaw,
    OrderNormalized,
    OrderWithCustomer,
    OrderEnriched,
)
from pipeline_src.schemas.customer import Customer
from pipeline_src.schemas.product import Product


@transform
class EnrichOrders(Transform):

    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    @expr_fn
    def normalized_total(value):
        return to_decimal(value, precision=12, scale=2)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())
        where(order.product_id.is_not_null())

        return OrderNormalized(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            total=self.normalized_total(order.total),
        )

    @after(normalize)
    def remove_negative_totals(self, *, df, spark, ctx):
        return df.where(F.col("total") >= 0)

    def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
        customer = self.customers.join_one(
            on=self.customers.id == order.customer_id,
            how=Join.LEFT,
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer(
            id=order.id,
            customer_id=order.customer_id,
            customer_name=customer.name,
            customer_tier=customer.tier,
            product_id=order.product_id,
            total=order.total,
        )

    def add_product(self, order: OrderWithCustomer) -> OrderEnriched:
        product = self.products.join_one(
            on=self.products.id == order.product_id,
            how=Join.LEFT,
        )

        where(product.id.is_not_null())

        return OrderEnriched(
            id=order.id,
            customer_id=order.customer_id,
            customer_name=order.customer_name,
            customer_tier=order.customer_tier,
            product_id=order.product_id,
            product_name=product.name,
            product_category=product.category,
            total=order.total,
        )

    @after(add_product, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
    def add_quality_columns(self, *, df, spark, ctx):
        return (
            df
            .withColumn("_has_customer", F.col("customer_name").isNotNull())
            .withColumn("_has_product", F.col("product_name").isNotNull())
        )
```

### Generated PySpark code

Structure generates one PySpark class per transform class.

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_src.transforms.order import EnrichOrders
from pipeline_generated.pyspark.schemas.order import (
    ORDER_RAW_SCHEMA,
    ORDER_NORMALIZED_SCHEMA,
    ORDER_WITH_CUSTOMER_SCHEMA,
    ORDER_ENRICHED_SCHEMA,
)
from pipeline_generated.pyspark.schemas.customer import CUSTOMER_SCHEMA
from pipeline_generated.pyspark.schemas.product import PRODUCT_SCHEMA
from pipeline_generated.pyspark.runtime.schema_assert import assert_schema, project_schema


class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()

    def run(
        self,
        *,
        orders: DataFrame,
        customers: DataFrame,
        products: DataFrame,
    ) -> DataFrame:
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")
        assert_schema(customers, CUSTOMER_SCHEMA, name="Customer", mode="strict")
        assert_schema(products, PRODUCT_SCHEMA, name="Product", mode="strict")

        # Subtransform: normalize
        df = orders.where(
            F.col("id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
        ).select(
            F.col("id").alias("id"),
            F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
            F.lower(F.trim(F.col("product_id"))).alias("product_id"),
            F.col("total").cast("decimal(12,2)").alias("total"),
        )

        df = self._impl.remove_negative_totals(df=df, spark=self.spark, ctx=self.ctx)
        assert_schema(df, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")

        # Subtransform: add_customer
        df = df.alias("order_normalized")
        customers_df = F.broadcast(customers.alias("customers"))
        df = df.join(
            customers_df,
            F.col("customers.id") == F.col("order_normalized.customer_id"),
            "left",
        ).select(
            F.col("order_normalized.id").alias("id"),
            F.col("order_normalized.customer_id").alias("customer_id"),
            F.col("customers.name").alias("customer_name"),
            F.col("customers.tier").alias("customer_tier"),
            F.col("order_normalized.product_id").alias("product_id"),
            F.col("order_normalized.total").alias("total"),
        )
        assert_schema(df, ORDER_WITH_CUSTOMER_SCHEMA, name="OrderWithCustomer", mode="strict")

        # Subtransform: add_product
        df = df.alias("order_with_customer")
        products_df = products.alias("products")
        df = df.join(
            products_df,
            F.col("products.id") == F.col("order_with_customer.product_id"),
            "left",
        ).where(
            F.col("products.id").isNotNull()
        ).select(
            F.col("order_with_customer.id").alias("id"),
            F.col("order_with_customer.customer_id").alias("customer_id"),
            F.col("order_with_customer.customer_name").alias("customer_name"),
            F.col("order_with_customer.customer_tier").alias("customer_tier"),
            F.col("order_with_customer.product_id").alias("product_id"),
            F.col("products.name").alias("product_name"),
            F.col("products.category").alias("product_category"),
            F.col("order_with_customer.total").alias("total"),
        )

        df = self._impl.add_quality_columns(df=df, spark=self.spark, ctx=self.ctx)
        assert_schema(df, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="allow_extra_columns")
        df = project_schema(df, ORDER_ENRICHED_SCHEMA)
        assert_schema(df, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
        return df
```

The generated code is longer than the source, but that is the point: Structure lets developers author compact schema logic while still producing explicit, reviewable PySpark.

## Performance Philosophy

Structure is intentionally strict. Compiled subtransforms must lower to Spark-plan-visible expressions. Unsupported Python operations are rejected at compile time instead of silently becoming Python UDFs, row-wise maps, or opaque callbacks.

This is a performance feature. Spark can optimize transformations only when work remains visible in the DataFrame logical plan. Projection, filtering, joins, predicate pushdown, column pruning, aggregation planning, and whole-stage code generation all depend on expressing work through Spark's relational expression model.

Arbitrary PySpark is still supported, but only through explicit hooks.

## Default Project Layout

```text
structure/
  src/
    pipeline_src/
      schemas/
      transforms/
  generated/
    pipeline_generated/
      pyspark/
        schemas/
        transforms/
        runtime/
        lineage/
```

`structure/src` and `structure/generated` are filesystem roots, not package names. Mark them as source roots in the IDE. Do not add `structure/__init__.py`; the top-level directory is a workspace container and should not shadow the installed Structure library package.

All paths and package names are configurable.

## Optional Configuration

Structure works by convention. For repeatable builds, use `pyproject.toml`:

```toml
[tool.structure]
source_dir = "structure/src"
generated_dir = "structure/generated"
source_package = "pipeline_src"
generated_package = "pipeline_generated"
lineage = "basic"
strict_performance = true
```

See `pyproject.seed.toml` for all defaults.

## CLI

```bash
structure check
structure compile
structure compile --fail-on-diff
structure explain pipeline_src.transforms.order.EnrichOrders
```

## Roadmap

- **v1:** projection, filtering, joins, typed intermediate schemas, generated PySpark classes, hooks, validation, basic LDJSON lineage, streaming-compatible transforms.
- **v2:** aggregations, windowing, advanced grouping, Spark higher-order functions, caching/persistence hints, join strategy annotations, richer lineage.
- **v3:** full streaming orchestration: `readStream`, `writeStream`, triggers, checkpoints, watermarks, and stateful policies.
