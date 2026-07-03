# Quick Reference

## Schema Classes

A schema class defines a named row contract by inheriting from `Structure` and declaring `field(...)`
attributes.

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    promotion_code = field(String(), nullable=True, alias="promo-code")
    total = field(String(), nullable=True)


class OrderWithCustomer(OrderRaw):
    customer_name = field(String(), nullable=True)
```

Use schema classes for inputs, intermediate rows, and outputs. Inheritance keeps shared fields explicit
without repeating declarations.

Use `alias=` when the Spark DataFrame column is not a Python identifier. Python code uses the field name,
while Spark schemas, validation, reads, and projection output use the alias. Aliases are schema-local unless
inherited, and Structure passes alias strings through to Spark without sanitizing them.

Reference: [schema declaration syntax](specifications/SchemaDeclarationSyntax.md),
[schema semantics](specifications/SchemaSemantics.md), and
[nullability and type coercion](specifications/NullabilityAndTypeCoercion.md).

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

Run the transform:

```python
session = StructureSession(spark=spark)

result = NormalizeOrders(
    orders=orders_df,
).run(session)

normalized = result.normalized
```

Structure can also generate PySpark code from transform classes for projects that prefer generated PySpark
code.

Transform classes may inherit reusable steps from undecorated `Transform` parents:

```python
class NormalizeBase(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)

    @transform(output=normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        ...


@transform
class PublishOrders(NormalizeBase):
    published = output(OrderPublished)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        ...
```

Parent steps run before child steps. Multiple parents run in the declared base-class order. An override can schedule a
parent implementation as a separate step with `super().normalize(order)`, `Base.normalize(self, order)`, or
`super(Base, self).normalize(order)`.

Reference: [DSL](specifications/DSL.md), [online execution](specifications/OnlineExecution.md), and
[PySpark code generation](specifications/PySparkCodeGeneration.md).

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

Reference: [DSL inputs](specifications/DSL.md) and
[source module rules](specifications/SourceModuleRules.md).

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

Most single-lane transforms need no method-level selectors. Declare intermediate lanes for named funnel
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

Here the compiler infers the input and lane sources from parameter types. The decorators name the intermediate
lanes; the final single output can be inferred from `publish` returning `OrderEnriched`.

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
    join_one(
        on=order.product_id == product.id,
        how=Join.LEFT,
    )

    accepted_order = OrderWithProduct.base(order)(product_name=product.name)
    audited_order = OrderWithProduct.base(order)(product_name=product.name)
    return accepted_order, audited_order
```

The first parameter is the driving DataFrame. Later parameters are relations and must be joined before their
fields are used. Joins and `where(...)` filters run once; each returned value is then projected into its named
output frame. Use `input=` for original input or intermediate lane declarations, and `output=` for
intermediate lane or final output declarations. Both options accept either one declaration or an ordered list.
The plural method options are retired.

Bare declarations resolve from the current source-order state. If a same-named lane already exists and its
current schema matches the method parameter, it wins over the original input. Use role selectors when the
distinction matters:

```python
@transform(input=input(orders), output=lane(orders))
def restart_from_raw(self, order: OrderRaw) -> OrderNormalized:
    ...

@transform(inout=lane(orders) | output(published))
def publish(self, order: OrderNormalized) -> OrderPublished:
    ...
```

`input(orders)` means the original runtime input. `lane(orders)` means the current working lane named
`orders`. `output(published)` means the final result declaration.

Reference: [DSL subtransforms](specifications/DSL.md),
[symbolic execution](specifications/SymbolicExecution.md), and
[execution semantics](specifications/ExecutionSemanticContract.md).

## Online Execution

Constructing a transform binds inputs without starting Spark work. Running it through a session executes the
configured runtime target.

```python
from structure import StructureConfig, StructureSession

config = StructureConfig.resolve(project_root=".")
session = StructureSession(spark=spark, ctx=ctx, config=config)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched = result.enriched
enriched_schema = result.schema.enriched
```

The session owns the caller-supplied Spark reference, optional hook context, resolved Structure configuration,
execution mode, and target backend selection.

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

Reference: [online execution](specifications/OnlineExecution.md) and
[execution semantic contract](specifications/ExecutionSemanticContract.md).

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

Reference: [PySpark code generation](specifications/PySparkCodeGeneration.md).

## Generated Schemas in Caller Code

Generated schema constants are ordinary PySpark `StructType` values. Caller code may import them for reads and
for pre-write validation/projection.

```python
from structure_generated.orders.pyspark.schemas.order import ORDER_ENRICHED_SCHEMA, ORDER_RAW_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema, project_schema

orders = spark.read.schema(ORDER_RAW_SCHEMA).parquet(source_path)

assert_schema(result, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
result = project_schema(result, ORDER_ENRICHED_SCHEMA)
result.write.mode("overwrite").parquet(target_path)
```

Structure does not own storage orchestration. Callers own `write`, `writeStream`, table creation,
partitioning, checkpoints, output modes, and storage options.

Reference: [PySpark code generation](specifications/PySparkCodeGeneration.md) and
[streaming compatibility](specifications/StreamingCompatibility.md).

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

`schema_and_constraints` is reserved for opt-in data-quality checks such as accepted values, ranges,
uniqueness, referential checks, freshness, and row-count policies. These checks are separate from schema shape
and may trigger Spark work when Structure supports them. Future constraints should bind to input,
intermediate, or output phases; the matching phase mode controls whether those constraints run.

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

Reference: [validation semantics](specifications/ValidationSemantics.md) and
[data quality constraints](specifications/DataQualityConstraints.md).

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

When filters and joins are mixed, Structure preserves the source order. A filter written before a join runs
before that join; a filter written after a join can reference the joined relation.

Reference: [DSL filtering](specifications/DSL.md) and
[symbolic execution](specifications/SymbolicExecution.md).

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

Inside a compiled subtransform, the driving row can be omitted when it is the intended source:

```python
def publish(self, order: OrderWithPromotion) -> OrderPublished:
    return project(OrderPublished)
```

Use a field list when the output should copy only selected source fields. The list names source-row fields;
the method return annotation still defines the output schema and field order.

```python
def audit(self, order: OrderRaw) -> OrderAudit:
    return project(order, ["tenant", "audit", "business"])
```

When copied fields need adjustments, use `SchemaClass.project(source)(...)` and override the changed fields.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized.project(order)(
        total=to_decimal(order.total, precision=12, scale=2),
        quantity=coalesce(order.quantity, 1),
    )
```

Generated code prefers explicit projection over `drop(...)` so the output schema is deterministic.

Reference: [schema semantics](specifications/SchemaSemantics.md) and
[PySpark code generation](specifications/PySparkCodeGeneration.md).

## Expressions

Structure expressions are compiler-visible and lower to Spark Column expressions. Use Python literals and the supported
operators directly:

```python
def add_flags(self, order: OrderRaw) -> OrderWithFlags:
    total = to_decimal(order.total, precision=12, scale=2)
    return OrderWithFlags(
        customer_id=upper(trim(order.customer_id)),
        size_tier=when(total >= 1000, "large").otherwise("standard"),
        is_small=total < 100,
        total_with_tax=total + order.tax,
        line_total=order.price * order.quantity,
    )
```

Supported v1 expression forms are field references, literals, `==`, `!=`, `<`, `<=`, `>`, `>=`, `+`, `-`, `*`,
boolean `&`, `|`, `~`, null checks, `null_safe_eq(...)`, `lower(...)`, `upper(...)`, `trim(...)`, `to_decimal(...)`,
`coalesce(...)`, and `when(...).otherwise(...)`.

Reference: [DSL expressions](specifications/DSL.md) and
[nullability and type coercion](specifications/NullabilityAndTypeCoercion.md).

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

Reference: [DSL expression helpers](specifications/DSL.md).

## Aggregations

Use `group_by(...)` inside a subtransform that returns an aggregate schema. Aggregate assignments stay
compiler-visible and lower to Spark `groupBy(...).agg(...)`.

```python
def product_daily_summary(self, order: OrderFulfillment) -> ProductDailySummary:
    group_by(
        tenant_id=order.tenant.tenant_id,
        product_id=order.product_id,
        order_date=order.business.order_date,
    )

    return ProductDailySummary(
        tenant=order.tenant,
        product_id=order.product_id,
        order_date=order.business.order_date,
        order_count=count(),
        distinct_customers=count_distinct(order.customer_id),
        units=sum(order.quantity),
        min_units=min(order.quantity),
        max_units=max(order.quantity),
        avg_units=avg(order.quantity),
        gross_total=sum(order.total),
    )
```

Supported aggregate helpers are `count()`, `count_distinct(...)`, `sum(...)`, `min(...)`, `max(...)`, and `avg(...)`.
`sum(...)` and `avg(...)` require numeric expressions. Nullable aggregate outputs must feed nullable fields or be
repaired explicitly.

## Higher-Order Helpers

Use `arr_transform(...)`, `arr_filter(...)`, `map_transform_values(...)`, and `map_filter(...)` for Spark-plan-visible
collection callbacks.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    tags = arr_filter(
        arr_transform(order.tags, lambda tag: lower(trim(tag))),
        lambda tag: tag.is_not_null(),
    )
    return OrderNormalized.project(order)(tags=tags)
```

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    attributes = map_filter(
        map_transform_values(order.attributes, lambda key, value: lower(trim(value))),
        lambda key, value: value.is_not_null(),
    )
    return OrderNormalized.project(order)(attributes=attributes)
```

Callbacks are symbolic: they are evaluated once against a Structure expression, not row-by-row in Python. Callback
bodies must return typed Structure expressions or typed literals. Python boolean control flow such as `tag and ...`
is rejected; combine symbolic predicates with `&`, `|`, and `~`.

## Joins

Use symbolic joins. Ref: [Join semantics](specifications/JoinSemantics.md) and
[analytical join coverage](specifications/AnalyticalJoinCoverage.md).

Implemented join forms in the default PySpark profile:

| Form | Shape | Use |
| --- | --- | --- |
| `join_one(...)` | select one right row | Lookup enrichment. |
| `join_one(..., dedupe=...)` | deterministic one-row lookup | Snapshot or versioned lookups. |
| `exists(...)` | filter current rows by a right-side match | Semi join semantics. |
| `not_exists(...)` | filter current rows by no right-side match | Anti join semantics. |
| `join_many(...)` | multiply current rows by right-side matches | One output row per match. |
| `temporal_one(...)` | select one right row by validity window | SCD-style or temporal lookup enrichment. |
| `as_of_one(...)` | select latest right row at or before a left time | Backward time-relative enrichment. |

Prefer inferred `join_one(...)` when the `on` clause names exactly one unjoined relation:

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    join_one(
        on=order.customer_id == customer.id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer.base(order)(
        customer_name=customer.name,
    )
```

For relation parameters and class input scopes, the documented style is a bare inferred join. When the `on` clause
names exactly one unjoined relation, later reads from that relation parameter or input scope use the joined scope.

Use existence predicates when the right side decides whether the current row survives but does not contribute
fields:

```python
where(exists(on=(product.tenant.tenant_id == order.tenant.tenant_id) & (product.id == order.product_id)))
where(
    not_exists(
        on=(blocked_product.tenant.tenant_id == order.tenant.tenant_id)
        & (blocked_product.product_id == order.product_id)
    )
)
```

Use `join_many(...)` when one current row should intentionally produce one output row per right-side match:

```python
join_many(
    on=(shipment.order_id == order.id),
    how=Join.INNER,
    strategy=JoinStrategy.SHUFFLE_HASH,
)
```

Use deterministic lookup dedupe when duplicate right-side rows exist but the business rule still selects one row:

```python
join_one(
    on=product.id == order.product_id,
    how=Join.LEFT,
    dedupe=JoinDedupe.latest_by(product.audit.ingested_at, ties=TiePolicy.ERROR),
)
```

Use `temporal_one(...)` when the right row must be valid at a current-row time. The default interval is
closed-open: `valid_from <= at < valid_to`, with null `valid_to` treated as open-ended.

```python
temporal_one(
    on=(customer.tenant.tenant_id == order.tenant.tenant_id)
    & (customer.id == order.customer_id),
    at=order.business.order_date,
    valid_from=customer.valid_from,
    valid_to=customer.valid_to,
    how=Join.LEFT,
    overlaps=OverlapPolicy.ERROR,
)
```

Generated PySpark:

```python
orders = orders.alias("order_normalized")
customers_df = F.broadcast(customers.alias("customers"))

orders = orders.join(
    customers_df,
    F.col("order_normalized.customer_id") == F.col("customers.id"),
    "left",
).select(
    F.col("order_normalized.id").alias("id"),
    # Additional inherited order fields are emitted explicitly here.
    F.col("customers.name").alias("customer_name"),
)
```

## Inheritance

When the output schema inherits the current row schema, use `SchemaClass.base(row)(...)` to copy inherited
fields and name only the joined fields.

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    join_one(
        on=order.customer_id == customer.id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer.base(order)(
        customer_name=customer.name,
    )
```

Reference: [schema inheritance](specifications/SchemaInheritance.md) and
[schema semantics](specifications/SchemaSemantics.md).

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

Hooks receive `self`, the selected lane parameter, `spark`, and `ctx`. Named input DataFrames are not passed
to hooks by default.

When a hook needs the original named inputs, opt in explicitly:

```python
@after(normalize, lane=orders, pass_inputs=True)
def check_against_raw_orders(self, *, orders, inputs, spark, ctx):
    raw = inputs.orders
    return orders
```

`inputs` is a read-only namespace matching the transform's declared input names. It contains the original
`run(...)` input DataFrames, not the current intermediate lane.

Select hook DataFrames explicitly with input, lane, or output declarations:

```python
@after(add_product, lane=audited)
def add_audit_columns(self, *, audited, spark, ctx):
    return audited.withColumn("_audited", F.lit(True))
```

Single-result hooks still name the selected lane explicitly.

Reference: [hook semantics](specifications/HookSemantics.md) and
[validation semantics](specifications/ValidationSemantics.md).

## Source and Generated Paths

Default filesystem layout:

```text
src/orders/...
generated/structure_generated/orders/...
```

Generated paths are used only when Structure is configured to emit PySpark code; online execution is the
default. These paths are configurable. Mark `src` and `generated` as source roots in the IDE.

Reference: [source module rules](specifications/SourceModuleRules.md),
[configuration schema](specifications/ConfigSchema.md), and
[PySpark code generation](specifications/PySparkCodeGeneration.md).

## Streaming Compatibility

Structure transforms operate on DataFrames. If the input DataFrame is streaming and every compiled operation
is supported by Spark Structured Streaming, the transform can run in a streaming pipeline.

Structure does not generate `readStream` or `writeStream` before v3; the caller owns streaming orchestration.

Reference: [streaming compatibility](specifications/StreamingCompatibility.md).

## Compatibility

Online and generated execution target ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs for
PySpark 3.5.x and 4.0.x by default:

```toml
execution_mode = "online"
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "ordinary"
```

Spark Connect uses `target_backend = "pyspark"` with `target_variant = "spark-connect"`. It is planned as an
experimental end-of-v2 variant for completed v1/v2 batch features, with full support gated by parity evidence.
See [Compatibility.md](Compatibility.md).

Reference: [compatibility policy](specifications/CompatibilityPolicy.md) and
[backend capabilities](specifications/BackendCapabilities.md).

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

`schema=` accepts a PySpark `StructType` or any object exposing `.schema`. Path and table generation accept
either `spark=...` or `session=StructureSession(...)`:

```python
StructureTools.schemas.generate(from_table="catalog.db.orders", spark=spark, to="OrderRaw")
StructureTools.schemas.generate(from_table="catalog.db.orders", session=session, to="OrderRaw")
```

The CLI prints generated source to stdout:

```bash
structure tools schemas generate --from-path data/orders.parquet --format parquet --to OrderRaw
structure tools schemas generate --from-table catalog.db.orders --to OrderRaw
```

The CLI command runs in its own Python process, so it needs a shell where PySpark is installed and Spark can
start. For Delta paths, the shell must include the user's usual Delta-capable Spark configuration. In managed
Spark notebooks or jobs, use the Python API with the existing `SparkSession` or `StructureSession`.

Schema generation preserves Spark shape: field names, field order, Spark types, nullability, arrays, maps,
decimals, and nested structs. It does not infer primary keys, descriptions, inheritance, or data-quality
constraints. When Spark field names are not Python identifiers, generated Structure fields use safe Python
names with `alias=...`.

Reference: [CLI](specifications/CLI.md) and
[schema declaration syntax](specifications/SchemaDeclarationSyntax.md).

## Planned Features

Implemented v2 analytical features include existence joins, `join_many(...)`, deterministic lookup dedupe,
temporal validity joins, aggregation/grouping, Spark higher-order array/map helpers, caching, and target capability
checks.

Remaining planned v2 features include:

- Window functions and deduplication helpers.
- Repartition and coalesce annotations.
- Backward as-of analytical joins.

These features remain explicit because Structure should not hide performance-sensitive choices.

Planned v2 adoption tooling also includes richer explain output, generated documentation artifacts for schemas
and transforms, production incremental compilation, and a pytest helper for compiler checks and generated-code
freshness.

Reference: [analytical join coverage](specifications/AnalyticalJoinCoverage.md),
[backend capabilities](specifications/BackendCapabilities.md), and
[alternative backends](specifications/AlternativeBackends.md).

## Next Steps

Get started: [GettingStarted.md](GettingStarted.md)

Browse deeper behavior definitions: [Reference.md](Reference.md)
