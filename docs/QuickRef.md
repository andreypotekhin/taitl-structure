# Quick Reference

## Schema Classes

A schema class defines a named row contract by inheriting from `Structure` and declaring `field(...)` attributes.

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    promotion_code = field(String(), nullable=True, alias="promo-code")
    total = field(String(), nullable=True)


class OrderWithCustomer(OrderRaw):
    customer_name = field(String(), nullable=True)
```

Use schema classes for inputs, intermediate rows, and outputs. Inheritance keeps shared fields explicit without
repeating declarations.

Use `alias=` when the Spark DataFrame column is not a Python identifier. Python code still uses the field name, while
Spark schemas, validation, reads, and projection output use the alias. Aliases are schema-local unless the field is
inherited, and Structure passes alias strings through to Spark without sanitizing them.

## Transform Classes

A transform class is declared with `@transform`.

```python
@transform
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        ...
```

Run transform:

```python
session = StructureSession(spark=spark)

result = NormalizeOrders(
    orders=orders_df,
).run(session)

normalized = result.normalized
```

Structure can also generate PySpark code from transform classes for projects that prefer generated PySpark code.

## Inputs

Inputs are named class attributes.

```python
orders = input(OrderRaw)
customers = input(Customer)
products = input(Product)
```

Generated `run(...)` methods use the same names.

```python
def run(self, *, orders, customers, products):
    ...
```

When more than one input has the same schema, select the intended source on the subtransform:

```python
orders_external = input(OrderRaw)
orders_internal = input(OrderRaw)

@transform(input=orders_external)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

## Subtransforms

Public instance methods with schema return annotations are compiled as subtransforms.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Subtransforms execute in source order.

```text
OrderRaw -> OrderNormalized -> OrderWithCustomer -> OrderEnriched
```

Most single-lane transforms need no method-level selectors. Declare intermediate lanes only when you want named funnel
stages, branches, or repeated schemas that need disambiguation:

```python
orders_raw = input(OrderRaw)
orders_normalized = lane(OrderNormalized)
orders_with_product = lane(OrderWithProduct)
published = output(OrderEnriched)

@transform(output=orders_normalized)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...

@transform(output=orders_with_product)
def add_product(self, order: OrderNormalized) -> OrderWithProduct:
    ...

def publish(self, order: OrderWithProduct) -> OrderEnriched:
    ...
```

Here the compiler infers the input and lane sources from parameter types. The decorators name the intermediate lanes;
the final single output can be inferred from `publish` returning `OrderEnriched`.

Subtransforms may declare additional schema parameters for relations used by the step. Bind repeated schemas
explicitly and return a fixed schema tuple when the shared join/filter work produces multiple results:

```python
@transform(
    input=[orders_external, products],
    output=[accepted, audited],
)
def add_product(
    self,
    order: OrderRaw,
    product: Product,
) -> tuple[OrderWithProduct, OrderWithProduct]:
    product = join_one(
        product,
        on=product.id == order.product_id,
        how=Join.LEFT,
    )

    accepted_order = OrderWithProduct.base(order)(product_name=product.name)
    audited_order = OrderWithProduct.base(order)(product_name=product.name)
    return accepted_order, audited_order
```

The first parameter is the driving DataFrame. Later parameters are relations and must be joined before their fields
are used. Joins and `where(...)` filters run once; each returned value is then projected into its named output frame.
Use `input=` for original input or intermediate lane declarations, and `output=` for intermediate lane or final output
declarations. Both options accept either one declaration or an ordered list. The plural method options are retired.

Bare declarations resolve from the current source-order state. If a same-named lane already exists and its current
schema matches the method parameter, it wins over the original input. Use role selectors when the distinction matters:

```python
@transform(input=input(orders), output=lane(orders))
def restart_from_raw(self, order: OrderRaw) -> OrderNormalized:
    ...

@transform(inout=lane(orders) | output(published))
def publish(self, order: OrderNormalized) -> OrderPublished:
    ...
```

`input(orders)` means the original runtime input. `lane(orders)` means the current working lane named `orders`.
`output(published)` means the final result declaration.

## Online Execution

Constructing a transform binds inputs without starting Spark work. Running it through a session executes the configured
runtime target.

```python
from structure import StructureSession

session = StructureSession(spark=spark, ctx=ctx)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched = result.enriched
enriched_schema = result.schema.enriched
```

The session owns Spark, optional hook context, resolved Structure configuration, execution mode, and target backend
selection.

Use `result.schema` when caller code needs an output Spark schema in online mode:

```python
result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched_schema = result.schema.enriched
same_schema = result.schema["enriched"]
result.enriched.write.mode("overwrite").parquet(target_path)
```

## Optional Generated PySpark

A source subtransform like this:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())

    return OrderNormalized(
        id=order.id,
        customer_id=lower(trim(order.customer_id)),
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

generates PySpark like this:

```python
orders = orders.where(
    F.col("id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
    F.col("total").cast("decimal(12,2)").alias("total"),
)
```

## Generated Schemas in Caller Code

Generated schema constants are ordinary PySpark `StructType` values. Caller code may import them for reads and for
pre-write validation/projection.

```python
from structure_generated.orders.pyspark.schemas.order import ORDER_ENRICHED_SCHEMA, ORDER_RAW_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema, project_schema

orders = spark.read.schema(ORDER_RAW_SCHEMA).parquet(source_path)

assert_schema(result, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
result = project_schema(result, ORDER_ENRICHED_SCHEMA)
result.write.mode("overwrite").parquet(target_path)
```

Structure does not own storage orchestration. Callers own `write`, `writeStream`, table creation, partitioning,
checkpoints, output modes, and storage options.

## Intermediate Validation

Structure validates intermediate schemas by default.

Project-wide defaults:

```toml
validate_intermediate = true
intermediate_validation_mode = "schema_only"
```

Full phase defaults:

```toml
validate_inputs = true
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
validate_outputs = true
output_validation_mode = "schema_only"
```

Disable intermediate schema validation project-wide:

```toml
validate_intermediate = false
```

Choose fuller validation only when the added Spark work is intentional:

```toml
intermediate_validation_mode = "schema_and_constraints"
```

`schema_and_constraints` is reserved for opt-in data-quality checks such as accepted values, ranges, uniqueness,
referential checks, freshness, and row-count policies. These checks are separate from schema shape and may trigger Spark
work when Structure supports them. Future constraints should bind to input, intermediate, or output phases; the matching
phase mode controls whether those constraints run.

```python
@transform(validate_intermediate=True)
class EnrichOrders(Transform):
    enriched = output(OrderEnriched)
    ...
```

Disable class-wide:

```python
@transform(validate_intermediate=False)
class EnrichOrders(Transform):
    enriched = output(OrderEnriched)
    ...
```

Disable for one method:

```python
@validate_output(False)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

## Filtering

Use `where(...)` inside subtransforms.

```python
def valid_orders(self, order: OrderRaw) -> OrderValid:
    where(order.id.is_not_null())
    where(order.total.is_not_null())

    return OrderValid(
        id=order.id,
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

Multiple `where(...)` calls are combined with logical AND.

## Add and Drop Columns

Add columns by returning a schema with more fields.

```python
class OrderWithFlags(Structure):
    id = field(String())
    total = field(Decimal(12, 2))
    is_large = field(Boolean())


def add_flags(self, order: OrderRaw) -> OrderWithFlags:
    total = to_decimal(order.total, precision=12, scale=2)
    return OrderWithFlags(
        id=order.id,
        total=total,
        is_large=total > 1000,
    )
```

Drop columns by returning a schema with fewer fields.

Use `project(...)` when the output copies same-name compatible fields from a source row.

```python
def publish(self, order: OrderWithPromotion) -> OrderPublished:
    return project(order, OrderPublished)
```

Use a field list when the output should copy only selected source fields. The list names fields on the source row; the
method return annotation still defines the output schema and field order.

```python
def audit(self, order: OrderRaw) -> OrderAudit:
    return project(order, ["tenant", "audit", "business"])
```

For compact filtered projection, chain `where(...).project(...)`.

```python
def publish_valid(self, order: OrderRaw) -> OrderPublished:
    return where(order.id.is_not_null()).project(order, OrderPublished)
```

When copied fields need adjustments, use `SchemaClass.project(source)(...)` and override only the changed fields.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    return OrderNormalized.project(order)(
        total=to_decimal(order.total, precision=12, scale=2),
        quantity=coalesce(order.quantity, 1),
    )
```

Generated code prefers explicit projection over `drop(...)` so the output schema is deterministic.

## Expression Helpers

Use `@expr_fn` for reusable compileable expressions.

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))
```

Class-local helpers do not take `self`, but can be called through `self`.

```python
customer_id=self.clean_id(order.customer_id)
```

## Joins

Use symbolic joins.

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    customer = join_one(
        customer,
        on=customer.id == order.customer_id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer.base(order)(
        customer_name=customer.name,
    )
```

Generated PySpark:

```python
orders = orders.alias("order_normalized")
customers_df = F.broadcast(customers.alias("customers"))

orders = orders.join(
    customers_df,
    F.col("customers.id") == F.col("order_normalized.customer_id"),
    "left",
).select(
    F.col("order_normalized.id").alias("id"),
    # Additional inherited order fields are emitted explicitly here.
    F.col("customers.name").alias("customer_name"),
)
```

## Inheritance

When the output schema inherits the current row schema, use `SchemaClass.base(row)(...)` to copy inherited fields and
name only the joined fields.

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    customer = join_one(
        customer,
        on=customer.id == order.customer_id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer.base(order)(
        customer_name=customer.name,
    )
```

## Hooks

Hooks are explicit PySpark escape hatches.

```python
@after(normalize, lane=orders)
def remove_negative_totals(self, *, orders, spark, ctx):
    return orders.where(F.col("total") >= 0)
```

Hook signature:

```python
def hook_name(self, *, selected_lane_name, spark, ctx):
    ...
```

Hooks receive `self`, the selected lane parameter, `spark`, and `ctx`. Named input DataFrames are not passed to hooks
by default.

When a hook needs the original named inputs, opt in explicitly:

```python
@after(normalize, lane=orders, pass_inputs=True)
def check_against_raw_orders(self, *, orders, inputs, spark, ctx):
    raw = inputs.orders
    return orders
```

`inputs` is a read-only namespace matching the transform's declared input names. It contains the original `run(...)`
input DataFrames, not the current intermediate lane.

Select hook DataFrames explicitly with input, lane, or output declarations:

```python
@after(add_product, lane=audited)
def add_audit_columns(self, *, audited, spark, ctx):
    return audited.withColumn("_audited", F.lit(True))
```

Single-result hooks still name the selected lane explicitly.

## Source and Generated Paths

Default filesystem layout:

```text
src/orders/...
generated/structure_generated/orders/...
```

Generated paths are only used shwn Structure used for generating PySpark code (not by default).
These paths are configurable. Mark `src` and `generated` as source roots in the IDE.

## Streaming Compatibility

Structure transforms operate on DataFrames. If the input DataFrame is streaming and all compiled operations are
supported by Spark Structured Streaming, the transform can be used in a streaming pipeline.

Structure does not generate `readStream` or `writeStream` before v3; the caller owns streaming orchestration.

## Compatibility

Online and generated execution target ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs for PySpark
3.5.x and 4.0.x by default:

```toml
execution_mode = "online"
target_pyspark = ">=3.5,<4.1"
```

Spark Connect support is planned for v4 unless it can be added earlier without changing the public DSL, generated class
API, or streaming orchestration contract. See `docs/Compatibility.md`.

## Schema Generation Tool

Generate starter Structure schema classes from live Spark schema metadata:

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

`schema=` accepts a PySpark `StructType` or any object exposing `.schema`. Path and table generation accept either
`spark=...` or `session=StructureSession(...)`:

```python
StructureTools.schemas.generate(from_table="catalog.db.orders", spark=spark, to="OrderRaw")
StructureTools.schemas.generate(from_table="catalog.db.orders", session=session, to="OrderRaw")
```

The CLI prints generated source to stdout:

```bash
structure tools schemas generate --from-path data/orders.parquet --format parquet --to OrderRaw
structure tools schemas generate --from-table catalog.db.orders --to OrderRaw
```

The CLI command runs in its own Python process, so it requires a shell where PySpark is installed and Spark can start.
For Delta paths, the shell must include the user's usual Delta-capable Spark configuration. In managed Spark notebooks
or jobs, use the Python API with the existing `SparkSession` or `StructureSession`.

Schema generation preserves Spark shape: field names, field order, Spark types, nullability, arrays, maps, decimals,
and nested structs. It does not infer primary keys, descriptions, inheritance, or data-quality constraints.
When Spark field names are not Python identifiers, generated Structure fields use safe Python names with `alias=...`.

## Planned Features

Planned v2 features include:

- Window functions and deduplication helpers.
- Aggregation and advanced grouping.
- Spark higher-order functions for arrays and maps.
- Caching and persistence annotations.
- Repartition and coalesce annotations.
- Join strategy annotations.
- `join_many(...)` and other row-multiplying or existence-oriented join forms.

These features remain explicit because Structure should not hide performance-sensitive choices.

Planned v2 adoption tooling also includes richer explain output, generated documentation artifacts for schemas and
transforms, production incremental compilation, and a pytest helper for compiler checks and generated-code freshness.
