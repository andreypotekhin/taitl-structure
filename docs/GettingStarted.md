# Getting Started

This guide builds a small but realistic Structure pipeline: normalize raw orders, filter invalid rows, enrich orders with customers and products, and generate PySpark code suitable for Airflow.

## 1. Install

```bash
pip install structure
```

For local development with Spark tests:

```bash
pip install structure[pyspark,dev]
```

## 2. Create a Project Layout

Structure defaults to this layout:

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
```

This keeps Structure source and generated code under one namespace and avoids generic top-level directories such as `generated/` that may collide with other tools.

IDE note: `structure/src` and `structure/generated` should be ordinary Python packages with `__init__.py` files. This keeps import resolution and jump-to-declaration behavior predictable. If a project already uses `structure` as another package name, configure different directories.

## 3. Define Schemas

```python
# structure/src/schemas/order.py

from structure import Schema, field, string, decimal


class OrderRaw(Schema):
    id = field(string, nullable=False)
    customer_id = field(string, nullable=False)
    product_id = field(string, nullable=False)
    total = field(string, nullable=True)


class OrderNormalized(Schema):
    id = field(string, nullable=False)
    customer_id = field(string, nullable=False)
    product_id = field(string, nullable=False)
    total = field(decimal(12, 2), nullable=True)


class OrderWithCustomer(Schema):
    id = field(string, nullable=False)
    customer_id = field(string, nullable=False)
    customer_name = field(string, nullable=True)
    customer_tier = field(string, nullable=True)
    product_id = field(string, nullable=False)
    total = field(decimal(12, 2), nullable=True)


class OrderEnriched(Schema):
    id = field(string, nullable=False)
    customer_id = field(string, nullable=False)
    customer_name = field(string, nullable=True)
    customer_tier = field(string, nullable=True)
    product_id = field(string, nullable=False)
    product_name = field(string, nullable=True)
    product_category = field(string, nullable=True)
    total = field(decimal(12, 2), nullable=True)
```

```python
# structure/src/schemas/customer.py

from structure import Schema, field, string


class Customer(Schema):
    id = field(string, nullable=False, primary_key=True)
    name = field(string, nullable=True)
    tier = field(string, nullable=True)
```

```python
# structure/src/schemas/product.py

from structure import Schema, field, string


class Product(Schema):
    id = field(string, nullable=False, primary_key=True)
    name = field(string, nullable=True)
    category = field(string, nullable=True)
```

## 4. Define a Transform

```python
# structure/src/transforms/order.py

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

## 5. Check and Compile

```bash
structure check
structure compile
```

Generated code appears under:

```text
structure/generated/
  schemas/
  transforms/
  runtime/
  lineage/
```

## 6. Use Generated Code

```python
from structure.generated.transforms.order import EnrichOrdersGenerated

result = EnrichOrdersGenerated(spark=spark).run(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
)
```

## 7. Use in Airflow

```python
from structure.generated.transforms.order import EnrichOrdersGenerated


def enrich_orders_task():
    orders = spark.read.parquet("/data/orders_raw")
    customers = spark.read.parquet("/data/customers")
    products = spark.read.parquet("/data/products")

    enriched = EnrichOrdersGenerated(spark=spark).run(
        orders=orders,
        customers=customers,
        products=products,
    )

    enriched.write.mode("overwrite").parquet("/data/orders_enriched")
```

## 8. Optional Configuration

Structure works with defaults. A user config only needs to specify settings that differ from defaults.

Preferred location:

```toml
# pyproject.toml

[tool.structure]
# Defaults are shown here for reference only.
# source_dir = "structure/src"
# generated_dir = "structure/generated"
# validate_intermediate = true
```

To generate a seed config that declares all defaults:

```bash
structure init --seed-config
```

Seed config:

```toml
[tool.structure]
source_dir = "structure/src"
generated_dir = "structure/generated"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.2"

validate_inputs = true
validate_intermediate = true
validate_outputs = true

lineage = "basic"
streaming_compatibility_checks = true
strict_performance = true

format_generated = true
fail_on_diff = false
```
