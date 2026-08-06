# Getting Started

This guide builds a small Structure transform: normalize order rows, validate required keys,
enrich with customer data, and run it through `StructureSession`. Generated PySpark remains available as
optional build output.

## Read this first

QuickRef: [QuickRef.md](QuickRef.md)

## 1. Install

```bash
poetry build
pip install dist/structure-[version]-py3-none-any.whl
```

If you plan to execute transforms against Spark locally, install PySpark separately:

```bash
pip install pyspark
```

For local development with test dependencies:

```bash
poetry install
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

`src` is the source filesystem root. `generated` is optional unless your project commits generated PySpark
artifacts. Generated modules mirror source import paths under `structure_generated`.

## 3. Define Schemas

```python
# src/orders/schemas/order.py

from structure import Schema
from structure.plugin.pyspark import *


class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    promotion_code = string(alias="promo-code")
    total = string()


class OrderNormalized(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    promotion_code = string()
    total = decimal(12, 2)


class OrderWithCustomer(OrderNormalized):
    customer_name = string()
    customer_tier = string()
```

`alias=` names the Spark DataFrame column when it differs from the Python field name. In this example,
`OrderRaw` expects `promo-code` in the input DataFrame, while `OrderNormalized` emits `promotion_code` because
aliases are schema-local unless inherited.

```python
# src/orders/schemas/customer.py

from structure import Schema
from structure.plugin.pyspark import *


class Customer(Schema):
    id = string(nullable=False)
    name = string()
    tier = string()
```

## 4. Define a Transform

```python
# src/orders/transforms/order.py

from structure import Transform, input, lane, output, raw
from structure.plugin.pyspark import *
from orders.schemas.order import OrderRaw, OrderNormalized, OrderWithCustomer
from orders.schemas.customer import Customer


class EnrichOrders(Transform):

    orders = input(OrderRaw)
    customers = input(Customer)
    enriched = output(OrderWithCustomer)

    def clean_id(self, value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())
        where(order.product_id.is_not_null())

        return OrderNormalized.project(order)(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            total=to_decimal(order.total, precision=12, scale=2),
        )

    @raw(inout=lane(orders) | lane(orders))
    def remove_negative_totals(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F

        return orders.where(F.col("total") >= 0)

    def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
        lookup_join(
            on=order.customer_id == customer.id,
            how="left",
            hint="broadcast",
        )
        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
        )
```

## 5. Run Transform

```python
from structure import (
    Schema,
    StructureConfig,
    StructureSession,
    StructureTools,
    Transform,
    input,
    lane,
    output,
    raw,
    step,
    transform,
)
from orders.transforms.order import EnrichOrders

session = StructureSession(spark=spark)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
).run(session)

enriched_df = result.enriched
```
Results are available as DataFrames in transform's declared outputs.

Use the next steps if you want generated PySpark code.

## 6. (Optional) Compile to disk

```bash
structure check
structure compile
```

Generated files will appear under:

```text
generated/structure_generated/
  orders/pyspark/
    schemas/
    transforms/
  runtime/
  traceability/  # compiler metadata, not runtime telemetry
```

## 7. (Optional) Inspect Generated PySpark

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

        # Step method: normalize
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

        # Step method: add_customer
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
        return orders
```

## 8. (Optional) Use Generated Code

Step is optional because it is simpler to execute the transform with `.run()` method, without generating the code.

```python
from structure_generated.store.pyspark.transforms.examples.store.transforms.orders.enrich import EnrichOrdersGenerated

result = EnrichOrdersGenerated(spark=spark).run(
    orders=orders_df,
    customers=customers_df,
)

enriched_df = result.enriched
```

## 9. Example use with Airflow

We can run a Transform as part of Airflow or other orchestrator - no code generation needed.

```python
from structure import (
    Schema,
    StructureConfig,
    StructureSession,
    StructureTools,
    Transform,
    input,
    lane,
    output,
    raw,
    step,
    transform,
)
from orders.transforms.order import EnrichOrders


def enrich_orders_task():
    orders = spark.read.parquet("/data/orders_raw")
    customers = spark.read.parquet("/data/customers")
    session = StructureSession(spark=spark)

    result = EnrichOrders(
        orders=orders,
        customers=customers,
    ).run(session)

    result.enriched.write.mode("overwrite").parquet("/data/orders_enriched")
```

## 10. (Optional) Configuration

Structure works by convention. Add TOML only when you need repeatable settings or non-default paths.

Minimal `pyproject.toml`:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
```

A complete default seed is provided in `pyproject.seed.toml`. Most projects should only specify settings that
differ from defaults.
