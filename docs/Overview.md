# Structure

**Structure** is a Python-to-PySpark runtime compiler which allows writing Spark data pipelines in Pythonic way, creating optimizer-friendly PySpark code behind the scenes. It can also be used as PySpark code generator: output Python-style schemas and transformations as PySpark code. 

## Why Structure?

Hand-written PySpark is powerful, but large pipelines often become difficult to maintain:

- Column names are often repeated as strings.
- Intermediate schemas are often implicit.
- Schema drift is hard to catch before runtime.
- Business logic can hide inside Python UDFs or row-wise callbacks.
- Airflow DAGs can become tightly coupled to transformation details.
- Generated or repeated transformation code is hard to review consistently.

Structure makes schemas and transformations explicit Python classes while keeping the emitted PySpark visible
to Spark's optimizer.



## Less Code, More Spark!

Structure reduces hand-maintained PySpark boilerplate. Pipelines express filtering, joins, projections, and
normalization in plain Python while Spark still sees optimizer-visible DataFrame logic.

Define schemas. Define transforms. Run.

### Example Schema

```python
from structure import Structure, field, String, Decimal


class OrderRaw(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)
    promotion_code = field(String(), nullable=True, alias="promo-code")
    total = field(String(), nullable=True)


class OrderNormalized(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)
    promotion_code = field(String(), nullable=True)
    total = field(Decimal(12, 2), nullable=True)


class OrderWithCustomer(OrderNormalized):
    customer_name = field(String(), nullable=True)
    customer_tier = field(String(), nullable=True)

    
class Customer(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    tier = field(String(), nullable=True)   
    

class Product(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=False)    
```

### Example Transform

```python
@transform
class EnrichOrders(Transform):

    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)
    enriched = output(OrderEnriched)

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

        return OrderNormalized.project(order)(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            total=self.normalized_total(order.total),
        )

    @after(normalize, lane=orders)
    def remove_negative_totals(self, *, orders, spark, ctx):
        return orders.where(F.col("total") >= 0)

    def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
        customer = join_one(
            on=order.customer_id == self.customers.id,
            how=Join.LEFT,
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
        )

    def add_product(self, order: OrderWithCustomer, product: Product) -> OrderEnriched:
        join_one(
            on=order.product_id == product.id,
            how=Join.LEFT,
        )

        where(product.id.is_not_null())

        return OrderEnriched.base(order)(
            product_name=product.name,
            product_category=product.category,
        )

    @after(add_product, lane=orders, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True)
    def add_quality_columns(self, *, orders, spark, ctx):
        return (
            orders
            .withColumn("_has_customer", F.col("customer_name").isNotNull())
            .withColumn("_has_product", F.col("product_name").isNotNull())
        )
```

### Running a Transform

Run a transform with `.run(session)`:

```python
from structure import StructureSession
from orders.transforms.order import EnrichOrders

session = StructureSession(spark=spark, ctx=ctx)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched_df = result.enriched
```

### Generated PySpark Code

Generate PySpark source when your project wants checked-in PySpark.

Example generated PySpark:

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
    ) -> TransformResult:
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")
        assert_schema(customers, CUSTOMER_SCHEMA, name="Customer", mode="strict")
        assert_schema(products, PRODUCT_SCHEMA, name="Product", mode="strict")

        # Subtransform: normalize
        orders = orders.where(
            F.col("id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
        ).select(
            F.col("id").alias("id"),
            F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
            F.lower(F.trim(F.col("product_id"))).alias("product_id"),
            F.col("total").cast("decimal(12,2)").alias("total"),
        )

        orders = self._impl.remove_negative_totals(orders=orders, spark=self.spark, ctx=self.ctx)
        assert_schema(orders, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")

        # Subtransform: add_customer
        orders = orders.alias("order_normalized")
        customers_df = F.broadcast(customers.alias("customers"))
        orders = orders.join(
            customers_df,
            F.col("order_normalized.customer_id") == F.col("customers.id"),
            "left",
        ).select(
            F.col("order_normalized.id").alias("id"),
            F.col("order_normalized.customer_id").alias("customer_id"),
            F.col("customers.name").alias("customer_name"),
            F.col("customers.tier").alias("customer_tier"),
            F.col("order_normalized.product_id").alias("product_id"),
            F.col("order_normalized.total").alias("total"),
        )
        assert_schema(orders, ORDER_WITH_CUSTOMER_SCHEMA, name="OrderWithCustomer", mode="strict")

        # Subtransform: add_product
        orders = orders.alias("order_with_customer")
        products_df = products.alias("products")
        orders = orders.join(
            products_df,
            F.col("order_with_customer.product_id") == F.col("products.id"),
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

        orders = self._impl.add_quality_columns(orders=orders, spark=self.spark, ctx=self.ctx)
        assert_schema(orders, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="allow_extra_columns")
        orders = project_schema(orders, ORDER_ENRICHED_SCHEMA)
        assert_schema(orders, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
        return orders
```

## Performance Focus

Structure is intentionally strict. Compiled subtransforms must lower to Spark-plan-visible expressions.

Unsupported Python operations are rejected at compile time. This is a performance feature: Spark can optimize transformations only when work remains visible in the DataFrame logical plan. Projection, filtering, joins, predicate pushdown, column pruning, aggregation planning, and whole-stage code generation all depend on expressing work through Spark's relational expression model.

For custom logic, create expression helpers with `@expr_fn`. This keeps expression logic compiler-visible and reusable.

Arbitrary PySpark is still supported, but only through explicit hooks. Hooks receive the underlying DataFrame(s) for arbitrary manipulation. Hooks are escape hatches: Structure calls them, records them as opaque boundaries, but does not treat their body as compiler-visible logic.

## Default Project Layout

For online execution (default):

```text
src/
  my_package/
    schemas/
    transforms/
```

For generated code executions:

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
    traceability/  # compiler metadata, not runtime telemetry
```

For example, `src/my_package/` generates `generated/structure_generated/my_package/pyspark/`.

Mark both `src` and `generated` as source roots in the IDE. The paths and package names are configurable.

## Configuration

Structure works by convention, so many projects need no configuration. Add `pyproject.toml` when paths, modes,
validation defaults, or build settings need to be explicit:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
traceability = "compiler"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
strict_performance = true
```

See `pyproject.seed.toml` for defaults.

## Compatibility

Structure targets Python 3.11+, PySpark 3.5.x and 4.0.x, Linux runtimes, and Linux/macOS/Windows development
environments.

Airflow can call online or generated transforms. It is not a Structure dependency.

Spark Connect support is scheduled for v4 unless it can be added earlier without changing the public DSL,
generated class API, generated-code review model, or streaming orchestration contract.

See [Compatibility.md](docs/Compatibility.md) for the full versioning and compatibility policy.

## CLI

The CLI supports local development and CI checks:

```bash
structure check
structure compile
structure compile --fail-on-diff
structure explain orders.transforms.order.EnrichOrders
# Generate schema class from data:
structure tools schemas generate --from-path data/orders.parquet --format parquet --to OrderRaw
```

## Tools

`StructureTools` can generate starter Structure schema classes from existing Spark schemes/DataFrames.

```python
from structure import StructureSession, StructureTools

code = StructureTools.schemas.generate(schema=orders_df.schema, to="OrderRaw")
code = StructureTools.schemas.generate(schema=orders_df, to="OrderRaw")

session = StructureSession(spark=spark)
code = StructureTools.schemas.generate(
    from_path="data/orders.parquet",
    format="parquet",
    session=session,
    to="OrderRaw",
)
```

The CLI variant prints generated source to stdout:

```bash
structure tools schemas generate --from-path data/orders.parquet \
  --format parquet --to OrderRaw
structure tools schemas generate --from-table catalog.db.orders --to OrderRaw
```

CLI schema generation needs a shell where PySpark is installed and Spark can start. Delta paths also need the
user's normal Delta-capable Spark configuration. In notebooks or managed Spark jobs, prefer the Python API
with an existing `StructureSession`.

Generated classes preserve Spark schema shape: field names, field order, types, nullability, arrays, maps,
decimals, and nested structs. They do not infer primary keys, descriptions, inheritance, or data-quality
constraints. Spark fields that are not Python identifiers get safe Python names with `alias=...`:

```python
promotion_code = field(String(), nullable=True, alias="promo-code")
```

Python code uses `promotion_code`; Spark schemas, validation, and projections use `promo-code`.

Aliases are schema-local, and Structure passes alias strings through to Spark without sanitizing them.



## Next Steps

Basic concepts: [Basics.md](Basics.md)

Get started: [GettingStarted.md](GettingStarted.md)



## Roadmap

The roadmap follows an IR-first path: prove strict online execution with optional generated code, grow into
mainstream analytical pipelines, take ownership of streaming orchestration, then add Spark Connect once the
ordinary PySpark contract is stable.

- **Initial release:** online PySpark execution by default, optional generated PySpark classes, projection,
  filtering, joins, typed intermediate schemas, hooks, validation, compiler provenance, static dataflow
  traceability, streaming-compatible transforms, diagnostic links, and setup checks.
- **v2:** mainstream analytical features: windowing, deduplication, aggregations, advanced grouping, Spark
  higher-order functions, caching/persistence/repartition hints, `join_many(...)`, richer explain output,
  generated docs, and pytest helpers.
- **v3:** streaming orchestration: `readStream`, `writeStream`, triggers, checkpoints, watermarks, output
  modes, and stateful policies.
- **v4:** Spark Connect support and backend capability reporting.
