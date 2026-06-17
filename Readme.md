# Structure

**Structure** is a Python DSL and code generator for schema-enforced, IDE-friendly data pipelines that compile to clean PySpark DataFrame code.

Structure lets developers describe large dataset processing as typed schema-to-schema transitions while generating Spark optimizer-visible Column and DataFrame expressions.

## Why Structure?

Hand-written PySpark is powerful, but large pipelines often become difficult to maintain:

- Column names are frequently embedded as strings.
- Schema drift is hard to catch early.
- Intermediate pipeline states are often implicit.
- Reusable logic can become hidden in Python functions, UDFs, or one-off DataFrame snippets.
- Airflow DAGs can become tightly coupled to transformation internals.
- Generated or repeated transformation logic is difficult to review consistently.

Structure addresses these problems by making schemas and transformations explicit while keeping generated execution code close to idiomatic PySpark.

## Performance Philosophy

Structure is strict by design.

Compiled subtransforms must lower to Spark-plan-visible DataFrame and Column expressions. Unsupported Python operations are rejected at compile time rather than silently becoming Python UDFs, row-wise maps, or opaque callbacks.

This is a performance and optimization feature. Spark can optimize transformations only when the logical plan remains visible. Projection, filtering, joins, predicate pushdown, column pruning, aggregation planning, join planning, and code generation all depend on work being expressed through Spark's relational expression model.

Arbitrary PySpark is still supported, but only through explicit hooks.

## Quick Example

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

from structure.src.schemas.order import (
    OrderRaw,
    OrderNormalized,
    OrderWithCustomer,
    OrderEnriched,
)
from structure.src.schemas.customer import Customer
from structure.src.schemas.product import Product


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

Structure generates a corresponding PySpark class:

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only when hooks exist

    def run(self, *, orders, customers, products):
        ...
```

If a transform has no hooks, generated code does not import or instantiate the source transform class.

## Suggested Project Layout

The default layout keeps Structure source and generated code under one namespace to avoid collisions with other libraries:

```text
structure/
  src/
    schemas/
      order.py
      customer.py
      product.py
    transforms/
      order.py
  generated/
    schemas/
    transforms/
    runtime/
    lineage/

airflow_dags/
  order_daily.py
```

These paths are configurable. If the project already has a top-level `structure` package or uses another layout, set different directories in `pyproject.toml`.

## Configuration

Configuration is optional. Defaults are supplied by Structure's seed config. User configuration only needs to specify settings that differ from defaults.

```toml
[tool.structure]
# Only override what differs from defaults.
# source_dir = "structure/src"
# generated_dir = "structure/generated"
```

To write all defaults for visibility:

```bash
structure init --seed-config
```

## CLI

```bash
structure check
structure compile
structure compile --fail-on-diff
structure explain structure.src.transforms.order.EnrichOrders
structure init --seed-config
```

## Airflow Usage

```python
from structure.generated.transforms.order import EnrichOrdersGenerated


def run_order_pipeline():
    result = EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
        orders=orders_df,
        customers=customers_df,
        products=products_df,
    )

    result.write.mode("overwrite").parquet("/data/order_enriched")
```

## Roadmap

### v1

Projection, filtering, joins, typed intermediate schemas, generated PySpark classes, hooks, schema validation, basic LDJSON lineage, streaming-compatible generated transforms, and build/CI integration.

### v2

Aggregations, windowing, deduplication helpers, higher-order function helpers for nested arrays/maps, caching and persistence hints, join strategy controls, advanced grouping, richer but compact lineage, and optional field-level lineage.

### v3

Full streaming orchestration, including generated `readStream`, `writeStream`, triggers, checkpoints, watermarks, and stateful streaming policies.

## License

TBD.
