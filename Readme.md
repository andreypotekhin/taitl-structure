# Structure

**Structure** is a Python DSL and runtime/compiler toolkit for schema-enforced, IDE-friendly data pipelines that run as
optimizer-visible PySpark DataFrame code.

Structure lets developers describe large dataset processing as typed schema-to-schema transformations while executing or
generating Spark optimizer-visible Column and DataFrame expressions.

## Why Structure?

Hand-written PySpark is powerful, but large pipelines often become difficult to maintain:

- Column names are frequently embedded as strings.
- Intermediate schemas are often implicit.
- Schema drift is hard to catch before runtime.
- Business logic can become hidden inside Python UDFs or row-wise callbacks.
- Airflow DAGs can become tightly coupled to transformation internals.
- Generated or repeated transformation code is hard to review consistently.

Structure addresses these problems by making schemas and transformations explicit while generating readable PySpark code that remains visible to Spark's optimizer.

## Less Source Code, More Spark Code

A Structure transform can express projection, filtering, normalization, and serial enrichment joins in compact
schema-oriented Python. By default, Structure runs that transform online through a `StructureSession`. Generated
PySpark is still available for provenance, review, debugging, and projects that deliberately choose generated
execution.

### Source Structure code

```python
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

        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
        )

    def add_product(self, order: OrderWithCustomer) -> OrderEnriched:
        product = self.products.join_one(
            on=self.products.id == order.product_id,
            how=Join.LEFT,
        )

        where(product.id.is_not_null())

        return OrderEnriched.base(order)(
            product_name=product.name,
            product_category=product.category,
        )

    @after(add_product, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
    def add_quality_columns(self, *, df, spark, ctx):
        return (
            df
            .withColumn("_has_customer", F.col("customer_name").isNotNull())
            .withColumn("_has_product", F.col("product_name").isNotNull())
        )
```

### Online execution

```python
from structure import StructureSession
from orders.transforms.order import EnrichOrders

session = StructureSession(spark=spark, ctx=ctx)

enriched = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)
```

The transform instance is a deferred invocation. Construction binds DataFrame inputs; `.run(session)` asks the session
to choose the configured runtime runner and execute the transform.

### Optional generated PySpark code

Structure can also generate one PySpark class per transform class.

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orders.transforms.order import EnrichOrders
from structure_generated.orders.pyspark.schemas.order import (
    ORDER_RAW_SCHEMA,
    ORDER_NORMALIZED_SCHEMA,
    ORDER_WITH_CUSTOMER_SCHEMA,
    ORDER_ENRICHED_SCHEMA,
)
from structure_generated.orders.pyspark.schemas.customer import CUSTOMER_SCHEMA
from structure_generated.orders.pyspark.schemas.product import PRODUCT_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema, project_schema


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

The generated code is longer than the source, but that is the point when generated mode is useful: Structure lets
developers author compact schema logic while still producing explicit, reviewable PySpark.

## Performance Focus

Structure is intentionally strict. Compiled subtransforms must lower to Spark-plan-visible expressions. Unsupported
Python operations are rejected at compile time instead of silently becoming Python UDFs, row-wise maps, or opaque
callbacks.

This is a performance feature. Spark can optimize transformations only when work remains visible in the DataFrame
logical plan. Projection, filtering, joins, predicate pushdown, column pruning, aggregation planning, and whole-stage
code generation all depend on expressing work through Spark's relational expression model.

For reusable custom logic, prefer `@expr_fn`. Expression helpers are the supported way to keep project-specific
expression logic compiler-visible and reusable across transforms.

Arbitrary PySpark is still supported, but only through explicit hooks. Hooks receive the current DataFrame by default;
advanced hooks can opt into original named input DataFrames with `pass_inputs=True`. Hooks are honest escape hatches:
Structure calls them, records them as opaque boundaries in lineage and explain output, and does not treat their body as
compiler-visible logic.

## Default Project Layout

```text
src/
  my_package/
    schemas/
    transforms/
generated/
  structure_generated/
    my_package/
      pyspark/
        schemas/
        transforms/
    runtime/
    lineage/  # compiler metadata, not runtime telemetry
```

`src` and `generated` are filesystem roots, not package names. Mark both as source roots in the IDE.
Generated modules mirror source import paths under the `structure_generated` namespace. For example,
`src/my_package/` generates under `generated/structure_generated/my_package/pyspark/`.

All paths and package names are configurable.

## Optional Configuration

Structure works by convention. For repeatable builds, use `pyproject.toml`:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
lineage = "compiler"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
strict_performance = true
```

See `pyproject.seed.toml` for all defaults.

## Compatibility

Structure targets Python 3.11+, PySpark 3.5.x and 4.0.x, Linux runtimes, and Linux/macOS development
environments. Airflow is supported as a caller of online or generated transforms, not as a hard dependency.

Spark Connect support is scheduled for v4 unless it can be added earlier without changing the public DSL, generated
class API, generated-code review model, or streaming orchestration contract.

See `docs/Compatibility.md` for the full versioning and compatibility policy.

## CLI

```bash
structure check
structure compile
structure compile --fail-on-diff
structure explain orders.transforms.order.EnrichOrders
```

## License

LGPL-2.1 + Ethical Use Policy

See Licence.md

## Roadmap

The roadmap follows an IR-first north star: the initial release proves that Structure can replace hand-maintained
PySpark boilerplate with strict online execution and optional generated-code workflow; v2 makes that workflow useful for
mainstream analytical pipelines; v3 takes ownership of streaming lifecycle concerns; v4 adds Spark Connect after the
ordinary PySpark contract is stable.

- **Initial release:** online PySpark execution by default, optional generated PySpark classes, projection, filtering,
  joins, typed intermediate schemas, hooks, validation, compiler provenance, static dataflow lineage,
  streaming-compatible transforms, diagnostic links, and setup checks.
- **v2:** windowing, deduplication, aggregations, advanced grouping, Spark higher-order functions,
  caching/persistence/repartition hints, `join_many(...)`, richer explain output, generated docs, and pytest helpers.
- **v3:** full streaming orchestration: `readStream`, `writeStream`, triggers, checkpoints, watermarks,
  output modes, and stateful policies.
- **v4:** Spark Connect support and backend capability reporting.
