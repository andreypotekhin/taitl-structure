# Getting Started

This guide builds a small but realistic Structure transform: normalize order rows, validate required keys, enrich with
customer data, and run it through `StructureSession`. Generated PySpark remains available as an optional artifact.

## 1. Install

```bash
pip install structure
```

For local development with test dependencies:

```bash
pip install structure[pyspark,dev]
```

## 2. Create Project Layout

Recommended default layout:

```text
src/
  orders/
    schemas/
      order.py
      customer.py
    transforms/
      order.py
generated/
  structure_generated/
    orders/
      pyspark/
```

`src` is the source filesystem root. `generated` is optional unless your project commits generated PySpark artifacts.
Generated modules mirror source import paths under `structure_generated`.

## 3. Define Schemas

```python
# src/orders/schemas/order.py

from structure import Structure, field, String, Decimal


class OrderRaw(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)
    total = field(String(), nullable=True)


class OrderNormalized(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=True)


class OrderWithCustomer(OrderNormalized):
    customer_name = field(String(), nullable=True)
    customer_tier = field(String(), nullable=True)
```

```python
# src/orders/schemas/customer.py

from structure import Structure, field, String


class Customer(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    tier = field(String(), nullable=True)
```

## 4. Define a Transform

```python
# src/orders/transforms/order.py

from pyspark.sql import functions as F

from structure import (
    Transform,
    transform,
    input,
    output,
    expr_fn,
    where,
    after,
    lower,
    trim,
    to_decimal,
    Join,
    JoinHint,
)
from orders.schemas.order import OrderRaw, OrderNormalized, OrderWithCustomer
from orders.schemas.customer import Customer


@transform
class EnrichOrders(Transform):

    orders = input(OrderRaw)
    customers = input(Customer)
    enriched = output(OrderWithCustomer)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())
        where(order.product_id.is_not_null())

        return OrderNormalized(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            total=to_decimal(order.total, precision=12, scale=2),
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
```

## 5. Run Online

```python
from structure import StructureSession
from orders.transforms.order import EnrichOrders

session = StructureSession(spark=spark)

enriched = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
).run(session)
```

Construction binds DataFrame inputs. Calling `.run(session)` executes the transform through the session's configured
runtime runner.

## 6. Check and Optionally Compile

```bash
structure check
structure compile
```

Generated files, when requested, appear under:

```text
generated/structure_generated/
  orders/pyspark/
    schemas/
    transforms/
  runtime/
  traceability/  # compiler metadata, not runtime telemetry
```

## 7. Inspect Optional Generated PySpark

Generated code is intentionally explicit.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()

    def run(self, *, orders, customers):
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")
        assert_schema(customers, CUSTOMER_SCHEMA, name="Customer", mode="strict")

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
        return df
```

The Structure source is shorter and schema-oriented. The generated PySpark is longer, explicit, and reviewable.

## 8. Use Generated Code

```python
from structure_generated.orders.pyspark.transforms.order import EnrichOrdersGenerated

enriched = EnrichOrdersGenerated(spark=spark).run(
    orders=orders_df,
    customers=customers_df,
)
```

## 9. Use from Airflow

```python
from structure import StructureSession
from orders.transforms.order import EnrichOrders


def enrich_orders_task():
    orders = spark.read.parquet("/data/orders_raw")
    customers = spark.read.parquet("/data/customers")
    session = StructureSession(spark=spark)

    enriched = EnrichOrders(
        orders=orders,
        customers=customers,
    ).run(session)

    enriched.write.mode("overwrite").parquet("/data/orders_enriched")
```

## 10. Optional Configuration

Structure works by convention. Add TOML only when you need repeatable settings or non-default paths.

Minimal `pyproject.toml`:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
```

A complete default seed is provided in `pyproject.seed.toml`. Most projects should only specify settings that differ from defaults.
