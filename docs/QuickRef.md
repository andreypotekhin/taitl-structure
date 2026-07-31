# Quick Reference

For exhaustive reference on supported APIs, PySpark parity, examples and semantic differences, see the
[API](API.md): [schemas](api/Schemas.api.md), [transforms](api/Transforms.api.md),
[expressions](api/Expressions.api.md), [joins](api/Joins.api.md), [aggregations](api/Aggregations.api.md),
[windows](api/Windows.api.md), [collections](api/Collections.api.md), and [streaming](api/Streaming.api.md).

## Schema Classes

A schema class defines a contract and compiles into PySpark schema (`StructType`/`StructField`).

```python
from structure import Schema
from structure.plugin.pyspark import *

class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = string(nullable=True)

class OrderNormalized(OrderRaw):
    pass

class OrderWithCustomer(OrderRaw):
    customer_name = string(nullable=True)
```

The schema classes are used for inputs and outputs of the Transforms (next).

Inheritance allows to reuse schemas and avoid repeat declarations of fields.

Use `alias=` when the Spark DataFrame column is not a valid Python identifier.

Reference: [schemas API](api/Schemas.api.md), [schema declaration syntax](reference/Schema.ref.md),
[schema semantics](reference/Schema.ref.md), and
[nullability and type coercion](reference/Schema.ref.md).

## Transform Classes

A transform class is declared by inheriting `Transform`.

```python
from structure import Transform, input, lane, output
from structure.plugin.pyspark import *

class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        return OrderNormalized.project(order)(
          total=to_decimal(order.total, precision=12, scale=2))
```

A 'step method' is transform method that receives and returns schema classes, like the `normalize` method above. A transform class may have multiple step methods, which are executed in the order of declaration.

Running the transform:

```python
session = StructureSession(spark=spark)

result = NormalizeOrders(
    orders=orders_df,
).run(session)

normalized_df = result.normalized
```

The run() method compiles the transform and invokes its steps methods.

Transforms may subclass other Transforms. In such case, parent transforms execute first:

```python
class Normalize(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        ...

class Publish(Normalize):
    published = output(OrderPublished)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        ...
```

Parent transform step methods run before child transform step methods. Multiple parents are allowed: their step methods run in the declared order (left to right). A step method override in child transform can call parent implementation: `super().normalize(order)`, `Base.normalize(self, order)`, or
`super(Base, self).normalize(order)`.

Step methods do not call other step methods directly. Attempt to do so, except for the override as shown above, will result in error.

Reference: [transforms API](api/Transforms.api.md), [DSL](background/DSL.back.md),
[execution](background/Execution.back.md), and
[transform inheritance and composition](background/DSL.back.md).

## Input/Output

Inputs are Transform fields that correspond to input (consumed) DataFrames.

```python
orders = input(OrderRaw)
customers = input(Customer)
products = input(Product)
```

When more than one input has the same schema, bind the step explicitly with `@step(input=...)`:

```python
orders_external = input(OrderRaw)
orders_internal = input(OrderRaw)

@step(input=orders_external)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Outputs are Transform fields that correspond to produced (output) DataFrames.

Reference: [transforms API](api/Transforms.api.md), [DSL inputs](background/DSL.back.md), and
[source module rules](background/PySparkCodeGeneration.back.md).

## Lanes

Intermediate lanes can be added to carry or branch processing stages. Most transforms don't need lanes.

```python
orders_raw = input(OrderRaw)
orders_normalized = lane(OrderNormalized)
orders_rejected = lane(OrderRaw)
published = output(OrderEnriched)
```

The compiler infers input and lane sources from step method parameter types. If that cannot be done (as with
`orders_raw`/`orders_rejected` above), disambiguate with `@step(...)` decorator:

```python
@step(output=orders_rejected)
def ignore_inactive_orders(self, order: OrderRaw) -> OrderRaw:
    ...
```

## Step methods

Public instance methods with schema returns are called step methods.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Step methods execute in the order of their declaration in the source.

```text
OrderRaw -> OrderNormalized -> OrderWithCustomer -> OrderEnriched
```

Step methods may take additional schemas as parameters, for instance, to join with another frame. They can also return multiple relations as a tuple:

```python
@step(output=[orders_with_product, orders_audited])
def add_product(
    self,
    order: OrderRaw,
    product: Product,
) -> tuple[OrderWithProduct, OrderRaw]:
    left_join(on=order.product_id == product.id)
    accepted_order = OrderWithProduct.base(order)(product_name=product.name)
    audited_order = order
    return accepted_order, audited_order
```

Here, the first relation parameter (`order`) is the driving lane. The second relation parameter (`product`) is an additional relation - it must be joined before use.

The returned values are mapped to the transform's outputs and lanes by schema class. Use `@step(...)` with
`input=`/`output=` to disambiguate.

In `input=`/`output=`  values, a same-named lane with matching schema wins over the original input. Role selectors like `lane()`, `input()`, `output()` can be used to further disambiguate, if that matters:

```python
@step(input=input(orders), output=lane(orders))
def restart_from_raw(self, order: OrderRaw) -> OrderNormalized:
    ...

@step(inout=lane(orders) | output(published))
def publish(self, order: OrderNormalized) -> OrderPublished:
    ...
```

`input(orders)` means the original runtime input. `lane(orders)` means the current working lane named
`orders`. `output(published)` means the final result declaration.

Reference: [transforms API](api/Transforms.api.md), [DSL step methods](background/DSL.back.md),
[symbolic execution](background/DSL.back.md), and
[execution semantics](background/Execution.back.md).

## Execution

Structure does not own storage orchestration. Callers own `write`, `writeStream`, table creation,
partitioning, checkpoints, output modes, and storage options.

Construct a transform object specifying applicable inputs. Running it triggers in-memory compilation and executes the compiled code.

```python
config = StructureConfig.resolve(project_root=".")
session = StructureSession(spark=spark, ctx=ctx, config=config)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched_df = result.enriched
```

The session owns the caller-supplied Spark reference, Structure configuration,
execution mode and compiled artifacts. It preserves the compiled code between transform and invocations. For instance, the subsequent construction of new insances `EnrichOrders` and repeat invocations of its .run() (on same session) do not trigger recompiling.

## Generated PySpark Code

For a source step method like this:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    return OrderNormalized(
        id=order.id,
        customer_id=lower(trim(order.customer_id)),
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

the generated PySpark code looks like this:

```python
orders = orders.where(
    F.col("id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
    F.col("total").cast("decimal(12,2)").alias("total"),
)
```

Reference: [transforms API](api/Transforms.api.md) and
[PySpark code generation](background/PySparkCodeGeneration.back.md).

## Filtering

Use `where(...)` to filter on relation.

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

Reference: [expressions API](api/Expressions.api.md), [DSL filtering](background/DSL.back.md), and
[symbolic execution](background/DSL.back.md).

## Add and Drop Columns

Add columns by returning a schema with more fields. Drop columns by returning a schema with fewer fields.

```python
class OrderWithFlags(OrderWithCustomer):
    is_large = boolean()

def add_flags(self, order: OrderWithCustomer) -> OrderWithFlags:
    total = to_decimal(order.total, precision=12, scale=2)
    return OrderWithFlags.base(order)(
        is_large=total > 1000,
    )
```

Use `SchemaClass.project(source)` to copy same-name compatible fields from a source row:

```python
def publish(self, order: OrderWithPromotion) -> OrderPublished:
    return OrderPublished.project(order)

# Same as above:
def publish(self, order: OrderWithPromotion) -> OrderPublished:
    project(order, OrderPublished)
    return order
```

Use a field list when the output should copy only selected source fields.

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

Reference: [transforms API](api/Transforms.api.md), [schema semantics](reference/Schema.ref.md), and
[PySpark code generation](background/PySparkCodeGeneration.back.md).

## Expressions

Structure supports expressions that lower to Spark Column expressions, and allows to use Python literals directly:

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

Supported expression forms are field references, literals, `==`, `!=`, `<`, `<=`, `>`, `>=`, `+`, `-`, `*`,
boolean `&`, `|`, `~`, null checks, `null_safe_eq(...)`, `contains(...)`, `like(...)`, `ilike(...)`, `rlike(...)`,
array/map indexing, `lower(...)`, `upper(...)`, `trim(...)`, `to_decimal(...)`, `coalesce(...)`, and
`cast(...)`, `astype(...)`, `try_cast(...)` (PySpark 4 profile), `substring(...)`, `split(...)`,
`regexp_replace(...)`, `regexp_extract(...)`, `length(...)`, `concat_ws(...)`, and `when(...).otherwise(...)`.
Additional String helpers include `initcap(...)`, `reverse(...)`, `translate(...)`, `instr(...)`, and
`levenshtein(...)`.
Struct fields may also be read with `struct_expr.get_field(name)`.
Temporal helpers include `date_add(...)`, `datediff(...)`, and `date_trunc(...)`.
Numeric helpers include `abs(...)`, `round(...)`, `ceil(...)`, and `floor(...)`.
Predicate helpers include `isnull(...)`, `isnotnull(...)`, and `isnan(...)`.

Reference: [expressions API](api/Expressions.api.md), [DSL expressions](background/DSL.back.md), and
[nullability and type coercion](reference/Schema.ref.md).

## Expression Methods

A Transform class can declare expression methods for reusable expressions. Expression methods are expected to have compileable code, and Structure will fail if it can't compile. Use optional decoration `@special(type="expr")` if demarcation is needed for clarity.

```python
@special(type="expr")
def clean_id(value):
    return lower(trim(value))
```

Expression methods do not take `self`, but can be called through `self`.

```python
customer_id=self.clean_id(order.customer_id)
```

Reference: [expressions API](api/Expressions.api.md) and [DSL expression helpers](background/DSL.back.md).

### Intentional Scalar Python UDFs

Use `@special(type="udf")` only for deliberately opaque, row-local Python logic that cannot be expressed with the
typed DSL. Declare both its Spark return type and nullability. Structure records `DSL-W0403` by default because Spark
cannot inspect or optimize the Python body; set `@transform(warn_on_udfs=False)` only after accepting that trade-off.

```python
class Publish(Transform):
    rows = input(Raw)
    published = output(Published)

    @special(type="udf", return_type=types.string(), nullable=False)
    def clean(value: str) -> str:
        return value.strip()

    def publish(self, row: Raw) -> Published:
        return Published(id=self.clean(row.id))
```

This is an ordinary-PySpark-only escape hatch, not an implicit fallback for unsupported expressions: Spark Connect
rejects Python UDF capability requirements. Keep the body scalar and self-contained. Generated modules delegate to
the source transform unless `generated_code_options` includes `embed_udfs`; embedding affects generated module
self-containment, not compiler visibility or warning policy.

## Aggregation

Use `group_by(...)` to return an aggregate schema. Aggregate
assignments stay compiler-visible and lower to Spark grouping operations.

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

Aggregate helpers include `count()`, `count_distinct(...)`, `sum(...)`, `min(...)`, `max(...)`,  `avg(...)`.
 and more: `bool_and(...)`, `bool_or(...)`, `stddev(...)`, `variance(...)`, `corr(...)`, `covar(...)`,
`approx_count_distinct(...)`, `approx_percentile(...)`, `collect_list(...)`, `collect_set(...)`, `first_value(...)`,
and `last_value(...)`. Aggregate helpers accept `where=...` for metric-local filters. Use trailing
`having(lambda out: ...)` or chained `group_by(...).having(lambda out: ...)` to filter aggregate output rows.

Use `rollup(...)` for hierarchical subtotals, `cube(...)` for all grouping-key combinations, and
`grouping_sets(...)` for exact subtotal layouts. Subtotal rows may omit some grouping keys, so nullable subtotal fields
or explicit labels are required.

```python
def revenue_rollup(self, order: OrderFulfillment) -> OrderRevenueRollup:
    rollup(
        tenant_id=order.tenant.tenant_id,
        product_category=order.product_category,
        order_date=order.business.order_date,
    )

    return OrderRevenueRollup(
        tenant_id=order.tenant.tenant_id,
        product_category=order.product_category,
        order_date=order.business.order_date,
        grouping_id=grouping_id(),
        category_subtotal=is_grouped(order.product_category),
        order_count=count(),
        large_order_count=count(where=order.is_large),
        large_units=sum(order.quantity, where=order.is_large),
        any_large_order=bool_or(order.is_large),
        quantity_stddev=stddev(order.quantity),
        quantity_median=approx_percentile(order.quantity, 0.5, accuracy=100),
        estimated_customers=approx_count_distinct(order.customer_id),
        first_customer_id=first_value(order.customer_id, order_by=order.quantity),
        customer_ids=collect_set(order.customer_id),
    )
```

Use `cube(...)` for all grouping-key combinations:

```python
cube(
    tenant_id=order.tenant.tenant_id,
    product_category=order.product_category,
    customer_tier=order.customer_tier,
)
return OrderProductCube(
    tenant_id=order.tenant.tenant_id,
    product_category=order.product_category,
    customer_tier=order.customer_tier,
    grouping_id=grouping_id(),
    order_count=count(),
    distinct_customers=count_distinct(order.customer_id),
    gross_total=sum(order.total),
)
```

Use `grouping_sets(...)` when only specific subtotal levels are useful:

```python
grouping_sets(
    (order.region, order.customer_id),
    (order.region,),
    (),
)
return OrderGroupingSetSummary(
    region=order.region,
    customer_id=order.customer_id,
    grouping_id=grouping_id(),
    region_grouped=is_grouped(order.region),
    customer_grouped=is_grouped(order.customer_id),
    order_count=count(),
    gross_total=sum(order.total),
)
```

Use `having(...)` for post-aggregate filters:

```python
group_by(customer_id=order.customer_id).having(
    lambda total: total.order_count > 1
)
return CustomerOrderSummary(
    customer_id=order.customer_id,
    order_count=count(),
    gross_total=sum(order.total),
)
```

Reference: [aggregations API](api/Aggregations.api.md),
[advanced analytical operations](background/DSL.back.md), [DSL](background/DSL.back.md),
[IR](background/PySparkCodeGeneration.back.md), [PySpark code generation](background/PySparkCodeGeneration.back.md), and
[streaming compatibility](background/StreamingCompatibility.back.md).

## Latest/Earliest Rows

Use `latest_by(...)` or `earliest_by(...)` when a grouped set of rows must keep one row per partition by an explicit
ordering expression. Use `dedupe_latest_by(...)` or `dedupe_earliest_by(...)` when the same deterministic selection is
best described as keyed deduplication.

```python
def latest_events(self, event: RawEvent) -> LatestEvent:
    dedupe_latest_by(event.sequence, partition_by=event.account_id)
    return LatestEvent(
        account_id=event.account_id,
        event_id=event.event_id,
        sequence=event.sequence,
    )
```

The PySpark target lowers these helpers to `row_number()` over
`Window.partitionBy(...).orderBy(...)`, keeps rank `1`, then drops the temporary rank column. `partition_by` is
required so the selection is reviewable, and the current public tie policy is `"error"`.

Streaming: selected-row helpers are batch-only. Keep streaming-safe ranking and top-N state policy in caller-owned
PySpark until Structure admits a symbolic state contract.

For complete, outcome-oriented examples, see the [Latest Rows recipe](recipes/LatestRows.md) and the
[Earliest Rows recipe](recipes/EarliestRows.md).

Reference: [aggregations API](api/Aggregations.api.md), [DSL](background/DSL.back.md),
[IR](background/PySparkCodeGeneration.back.md),
[PySpark code generation](background/PySparkCodeGeneration.back.md), and
[streaming compatibility](background/StreamingCompatibility.back.md).

## Window Projection Functions

Use `row_number(...)`, `rank(...)`, `dense_rank(...)`, `lag(...)`, `lead(...)`, and rolling metric helpers when a
projected output field needs a Spark-visible analytical window value.

```python
def rank_events(self, event: RawEvent) -> RankedEvent:
    return RankedEvent(
        account_id=event.account_id,
        event_id=event.event_id,
        row_number=row_number(partition_by=event.account_id, order_by=event.sequence),
        rank=rank(partition_by=event.account_id, order_by=event.sequence, descending=True),
        previous_sequence=lag(event.sequence, partition_by=event.account_id, order_by=event.sequence),
        next_sequence=lead(event.sequence, partition_by=event.account_id, order_by=event.sequence),
        rolling_total=rolling_sum(event.amount, partition_by=event.account_id, order_by=event.sequence, preceding=6),
        rolling_average=rolling_avg(event.amount, partition_by=event.account_id, order_by=event.sequence, preceding=6),
    )
```

`partition_by` and `order_by` each accept one expression or a list/tuple of expressions. Use `asc_nulls_first()`,
`asc_nulls_last()`, `desc_nulls_first()`, or `desc_nulls_last()` on an order key when null placement matters. Set
`descending=True` when every inline order key should descend. `lag(...)` and `lead(...)` default to offset `1`; pass
`offset=...` and `default=...` when needed. `rolling_sum(...)`, `rolling_avg(...)`, `rolling_min(...)`, and
`rolling_max(...)` require `preceding=...`, the number of prior rows included with the current row. These helpers render
as PySpark window expressions in the projection, not Python UDFs.

Use reusable `window(...)` specification when several output fields share partition, ordering, and frame rules:

```python
def customer_window(self, order: OrderFulfillment) -> OrderCustomerWindow:
    customer_window = window(
        partition_by=order.customer_id,
        order_by=order.quantity,
        frame=rows_between(preceding(2), current_row()),
    )

    return OrderCustomerWindow(
        order_id=order.id,
        percent_rank=percent_rank(over=customer_window),
        cume_dist=cume_dist(over=customer_window),
        quantity_tile=ntile(2, over=customer_window),
        second_order_id=nth_value(order.id, 2, over=customer_window),
        running_units=window_sum(order.quantity, over=customer_window),
        running_avg_units=window_avg(order.quantity, over=customer_window),
    )
```

Reusable windows require explicit frames such as `rows_between(preceding(2), current_row())` or
`range_between(preceding(10), current_row())`. Aggregate helpers also include `window_bool_and`, `window_bool_or`,
`window_stddev`, `window_variance`, `window_collect_list`, and `window_collect_set`. Spark does not permit distinct
window aggregates; use grouped `count_distinct(...)` instead.

Streaming: analytical window helpers are batch-only. Use event-time `window(...)` or `session_window(...)` grouping for
the admitted Structured Streaming aggregate shapes.

Reference: [windows API](api/Windows.api.md),
[advanced analytical operations](background/DSL.back.md), [DSL](background/DSL.back.md),
[IR](background/PySparkCodeGeneration.back.md), [PySpark code generation](background/PySparkCodeGeneration.back.md), and
[streaming compatibility](background/StreamingCompatibility.back.md).

## Duplicate Rows

Use `distinct(relation)` for exact duplicate row removal over a relation. It is a synonym for
`drop_duplicates(relation)`.

```python
def unique_events(self, event: RawEvent) -> RawEvent:
    distinct(event)
    return RawEvent.project(event)
```

`drop_duplicates(...)` accepts a list of typed field expressions for PySpark-compatible subset dedupe. The relation is
inferred when all fields come from the same relation:

```python
def unique_accounts(self, event: RawEvent) -> RawEvent:
    drop_duplicates(event.account_id)
    return RawEvent.project(event)
```

Dedupe operations run in source order: before a relation is joined they prepare that relation's
source; after a join they apply to the active joined frame using the specified relation fields.

For a streaming frame, declare `watermark(event_time, delay=...)` first: ordinary `drop_duplicates(...)` then uses
bounded `dropDuplicatesWithinWatermark` rather than forever-global dedupe. Use
`drop_duplicates_within_watermark(...)` when the streaming-only intent should be explicit.

Use `exactly_one(relation)` before joining a policy/configuration relation that must contain one row:

```python
exactly_one(policy)
cross_join(policy, allow_cartesian=True)
```

The assertion preserves the relation on success. Zero or multiple rows fail during Spark evaluation with `REL-E0701`;
Structure does not collect the relation on the driver or choose an arbitrary first row. This helper is batch-only and
ordinary-PySpark-only.

When the selected row must be deterministic, prefer `dedupe_latest_by(...)` or `dedupe_earliest_by(...)`
with an explicit ordering and tie policy.

```python
def latest_events(self, event: RawEvent) -> RawEvent:
    dedupe_latest_by(event.sequence, partition_by=event.account_id)
    return RawEvent.project(event)
```

Streaming: `distinct(...)` remains batch-only. For streaming dedupe, declare a watermark first and use
`drop_duplicates(...)` or `drop_duplicates_within_watermark(...)`; the caller owns the required output mode.

Use `select_first_qualified(...)` when a relation contains several candidate rows and one eligible row should survive
per declared business key:

```python
def choose_feedback(self, option: FeedbackOption) -> FeedbackOption:
    selected = select_first_qualified(
        option.request_id,
        option.document_id,
        where=option.has_signal,
        order_by=option.priority.asc(),
        missing="allow",
    )
    return FeedbackOption.project(selected)
```

Keys must be declared field references. Tied eligible candidates fail with `REL-E0705`; `missing="error"` also fails
when a key has no eligible candidate. The helper is batch-only for streaming inputs.

Reference: [aggregations API](api/Aggregations.api.md), [DSL](background/DSL.back.md),
[IR](background/PySparkCodeGeneration.back.md),
[PySpark code generation](background/PySparkCodeGeneration.back.md), and
[streaming compatibility](background/StreamingCompatibility.back.md).

## Higher-Order Functions

Use `arr_transform(...)`, `arr_filter(...)`, `map_transform_values(...)`, and `map_filter(...)` for Spark-optimizer-visible
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

Array and map helpers can be combined into one compiled projection:

```python
normalized_tags = arr_transform(row.tags, lambda tag: lower(trim(tag)))
priority_tags = arr_filter(normalized_tags, lambda tag: tag == "priority")
paired_tags = arr_zip_with(row.tags, normalized_tags, lambda raw, clean: clean)
normalized_attributes = map_filter(
    map_transform_keys(
        map_transform_values(row.attributes, lambda key, value: lower(trim(value))),
        lambda key, value: lower(trim(key)),
    ),
    lambda key, value: value.is_not_null(),
)
entries = map_entries(normalized_attributes)

return OrderCollectionProfile(
    tag_count=size(row.tags),
    contains_priority=array_contains(row.tags, "priority"),
    contains_region=map_contains_key(row.extra_attributes, "Region"),
    default_tags=array("priority", "standard"),
    repeated_tags=array_repeat("priority", 2),
    all_tags=array_union(row.tags, row.extra_tags),
    tags_without_extra=array_except(row.tags, row.extra_tags),
    first_tag=element_at(row.tags, 1),
    safe_tag=try_element_at(row.tags, 2),
    normalized_tags=arr_distinct(priority_tags),
    sorted_tags=arr_sort_by(paired_tags, lambda tag: tag),
    flat_tags=arr_flatten(row.nested_tags),
    score_total=arr_aggregate(row.scores, 0, lambda acc, item: acc + item),
    tag_position=arr_position(row.tags, "priority"),
    has_priority=arr_exists(normalized_tags, lambda tag: tag == "priority"),
    all_tags_present=arr_forall(normalized_tags, lambda tag: tag.is_not_null()),
    normalized_attributes=normalized_attributes,
    zipped_attributes=map_zip_with(
        row.attributes,
        normalized_attributes,
        lambda key, left, right: coalesce(right, left),
    ),
    attribute_keys=map_keys(normalized_attributes),
    attribute_values=map_values(normalized_attributes),
    roundtrip_attributes=map_from_entries(entries),
    merged_attributes=map_concat(row.attributes, row.extra_attributes),
)
```

Callbacks are symbolic: they are evaluated once against a Structure expression, not row-by-row in Python. Callback
bodies must return typed Structure expressions or typed literals. Python boolean control flow such as `tag and ...`
is rejected; combine symbolic predicates with `&`, `|`, and `~`.

Reference: [collections API](api/Collections.api.md),
[advanced analytical operations](background/DSL.back.md), [DSL](background/DSL.back.md), and
[backend capabilities](background/BackendCapabilities.back.md).

## Joins

Implemented join forms in the default PySpark profile:

| Form | Shape | Use |
| --- | --- | --- |
| `lookup_join(...)` | select one right row | Lookup enrichment. |
| `lookup_join(..., dedupe=...)` | deterministic one-row lookup | Snapshot or versioned lookups. |
| `exists(...)` | filter current rows by a right-side match | Semi join semantics. |
| `not_exists(...)` | filter current rows by no right-side match | Anti join semantics. |
| `inner_join(...)` | multiply current rows by right-side matches | One output row per match. |
| `rowset_join(...)` | broad rowset join | Right, full, cross, non-equi, or disjunctive joins. |
| `temporal_one(...)` | select one right row by validity window | SCD-style or temporal lookup enrichment. |
| `as_of_one(...)` | select latest right row at or before a left time | Backward time-relative enrichment. |

Prefer inferred `left_join(...)` for ordinary enrichment when the `on` clause names exactly one unjoined relation:

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    left_join(
        on=order.customer_id == customer.id,
        hint="broadcast",
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

Use `inner_join(...)` when one current row should intentionally produce one output row per right-side match:

```python
inner_join(
    on=(shipment.order_id == order.id),
    strategy="shuffle_hash",
)
```

For same-named keys, write `inner_join(on="order_id")` or `left_join(on=["tenant_id", "order_id"])`; Structure
expands the shorthand to typed equality conditions.

Use rowset joins when the join can admit right-only rows, left-only rows, or a Cartesian product:

```python
full_join(on=customer.id == order.customer_id)
right_join(on=customer.id == order.customer_id)
cross_join(calendar_day, allow_cartesian=True)
```

`left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and `cross_join(...)` are shortcuts over
`rowset_join(...)`. Predicate shortcuts can be bare when the right relation is unambiguous.

See `examples/store/transforms/rowset_join.py` for a generated example covering `full_join(...)`, `right_join(...)`,
and `cross_join(...)`.

Right and full joins can produce rows without a current left row. Build outputs with explicit projection or explicit
constructors, and repair nullable sides with helpers such as `coalesce(...)` when the target field is required:

```python
return OrderCustomerReconciliation(
    tenant_id=coalesce(order.tenant.tenant_id, customer.tenant.tenant_id),
    order_id=order.id,
    customer_id=customer.id,
    match_status=coalesce(customer.tier, "unmatched"),
)
```

Cross joins require `allow_cartesian=True` and do not accept `on`. Non-equi and disjunctive predicates are supported
when every predicate part is a compileable symbolic expression. Raw SQL strings and raw column-name strings are
rejected.

Use deterministic lookup dedupe when duplicate right-side rows exist but the business rule still selects one row:

```python
lookup_join(
    on=product.id == order.product_id,
    how="left",
    dedupe=JoinDedupe.latest_by(product.audit.ingested_at, ties="error"),
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
    how="left",
    overlaps="error",
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

Ref: [joins API](api/Joins.api.md), [Join semantics](background/DSL.back.md),
[analytical join coverage](background/DSL.back.md), and
[full PySpark join support](background/DSL.back.md).

## Inheritance

### Schema Inheritance

Schema classes can subclass other schema classes.
This helps to avoid duplication and allows establishing schema hierarchies.

When constructing a subclass schema object, use `.base(row)(...)` to copy inherited fields from a base class instance:

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    left_join(on=order.customer_id == customer.id)
    return OrderWithCustomer.base(order)(
        customer_name=customer.name,
    )
```

Multiple-inherited schema classes compose left to right.
The `.base()` method allows for multiple bases:

```python
flags = PublicationFlags(
    has_customer=order.customer_name.is_not_null(),
    has_product=order.product_name.is_not_null(),
)
return OrderPublished.base(order, flags)
```

To copy fields from unrelated/non-base classes, use `.project()`.
The method copies same-named fields of compatible type. It can accept multiple sources; when more than one source
provides a target field, provide that field explicitly to make the chosen lineage clear.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized.project(order)(
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

You can combine inherited-field copying with same-name projection for fields introduced by the child schema:

```python
def plan(self, demand: OrderDemand, inventory: InventoryPosition) -> FulfillmentOption:
    inner_join(inventory, on=demand.product_id == inventory.product_id)
    return FulfillmentOption.base(demand).project(inventory)(
        available_to_promise=inventory.on_hand_quantity - inventory.reserved_quantity,
    )
```

```python
def combine(self, order: OrderRaw, customer: Customer) -> OrderCustomer:
    return OrderCustomer.project(order, customer)(
        customer_id=customer.id,  # explicit because both rows provide an id
    )
```

Reference: [schemas API](api/Schemas.api.md), [schema inheritance](reference/Schema.ref.md),
[schema declaration syntax](reference/Schema.ref.md), and
[schema semantics](reference/Schema.ref.md).

### Transform Inheritance

Transform classes can subclass other Transforms. They inherit inputs, lanes, outputs, hooks, helpers, and step methods
from parent class. Parent transforms run before child transform; a child method with the same name overrides
the inherited scheduled step. Multiple inheritance is allowed, in which case parents run left-to-right before
children. Python rules for resolving diamond inheritance shapes are observed.

```python
class Normalize(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)

    @step(output=normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(
            id=lower(trim(order.id)),
            customer_id=lower(trim(order.customer_id)),
        )

class Publish(Normalize):
    published = output(OrderPublished)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        return OrderPublished.project(order)
```

## Transform Composition

The transforms can also be composed into pipelines using `.to(...)` method.

This is an alternative to inheritance, providing more encapsulation (transforms are opaque to each other,
only connected through inputs/outputs) and allowing to combine independent transforms.

```python
result = (
    NormalizeOrders(orders=orders_df)
    .to(AddProduct(products=products_df))
    .to(PublishOrders())
    .run(session)
)

published_df = result.published
```

Composition hooks the inputs of downstream (following) transform to outputs of upstream transform, and to constructor
arguments.

For instance, in the above example, `AddProduct` initializes its 'products' input from constructor argument, and
other inputs, like 'orders' from the upstream `NormalizeOrders` transform outputs.

Use output aliases when the upstream declaration name is implementation-oriented but downstream transforms should
consume a stable name:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized).alias("orders")
```

Use invocation rename when the transform class cannot be changed:

```python
NormalizeOrders(orders=orders_df).rename(normalized="orders").to(AddProduct(products=products_df))
```

Boundary aliases and renames affect Structure input/output matching and result lookup. They do not rename Spark
columns; use schema field `alias=...` for column names and Spark DataFrame `.alias(...)` for relation names.

If an input is specified in constructor that already exists amongh upstream transform outputs, Structure
interprets it as a conflict and fails with an error.

The `.to(...)` method does not allow to bind to transform's lanes. Hook-bearing transforms are currently rejected.

Alternative notations:

```python
result = (
    Transform.to(NormalizeOrders(orders=orders_df))
    .to(AddProduct(products=products_df))
    .to(PublishOrders())
    .run(session)
)
```

```python
result = (
    Transform.to(
        NormalizeOrders(orders=orders_df),
        AddProduct(products=products_df),
        PublishOrders())
    .run(session)
)
```

For generated PySpark, wrap the pipeline in one transform field:

```python
class OrderPipeline(Transform):
    orders = input(OrderRaw)
    products = input(Product)

    pipeline = Transform.to(
        NormalizeOrders(orders=orders),
        AddProduct(products=products),
        PublishOrders(),
    )
```

Reference: [transforms API](api/Transforms.api.md),
[transform inheritance and composition](background/DSL.back.md),
[DSL](background/DSL.back.md), and [execution semantics](background/Execution.back.md).

## Hooks

Hooks are explicit PySpark escape hatches.

```python
@raw(inout=lane(orders) | lane(orders))
def remove_negative_totals(self, *, orders, spark, ctx):
    return orders.where(F.col("total") >= 0)
```

Hook signature:

```python
def hook_name(self, *, selected_lane_name, spark, ctx):
    ...
```

Hooks receive `self`, their explicitly bound DataFrame parameters, `spark`, and `ctx`.

When a hook needs an original named input, select it explicitly:

```python
@raw(inout=input(orders) | lane(orders))
def check_against_raw_orders(self, *, orders, spark, ctx):
    raw = orders
    return orders
```

`input(orders)` selects the original `run(...)` input; `lane(orders)` selects the current intermediate lane.

Select hook DataFrames explicitly with input, lane, or output declarations:

```python
@raw(inout=lane(audited) | output(audited))
def add_audit_columns(self, *, audited, spark, ctx):
    return audited.withColumn("_audited", F.lit(True))
```

Single-result hooks still name the selected lane explicitly.

Reference: [transforms API](api/Transforms.api.md), [hook semantics](background/HookSemantics.back.md), and
[validation semantics](reference/Schema.ref.md).

## Schema Validation

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

Reference: [schemas API](api/Schemas.api.md), [validation semantics](reference/Schema.ref.md), and
[data quality constraints](reference/Schema.ref.md).

## Streaming

Structure transforms operate on DataFrames. If the input DataFrame is streaming and every compiled operation
is supported by Spark Structured Streaming, the transform can run in a streaming pipeline.

Declare streaming sources on the inputs with `input(..., streaming=True)`. That input metadata makes the transform
streaming automatically, so `@transform(streaming=True)` is optional and may be kept only for emphasis.

Structure admits row-local projection/filter, stream-static joins, watermarks, event-time and session-window
aggregation, bounded dedupe, and admitted bounded stream-stream joins. It does not generate `readStream` or
`writeStream`; the caller owns sources, sinks, checkpoints, triggers, output modes, and query lifecycle.

Reference: [streaming API](api/Streaming.api.md) and
[streaming compatibility](background/StreamingCompatibility.back.md).

## Source and Generated Paths

Default filesystem layout:

```text
src/orders/...
generated/structure_generated/store/...
```

Generated paths are used only when Structure is configured to emit PySpark code; execution is the
default. These paths are configurable. Mark `src` and `generated` as source roots in the IDE.

Reference: [source module rules](background/PySparkCodeGeneration.back.md),
[configuration schema](background/CLI.back.md), and
[PySpark code generation](background/PySparkCodeGeneration.back.md).

### Disk-less Environments

In notebooks and other paste-and-run environments, compile trusted source text without writing it to a project tree:

```python
sources = StructureSources.files(
    {
        "orders/schemas.py": schema_text,
        "orders/transforms.py": transform_text,
    }
)
session = StructureSession(spark=spark, config=StructureConfig.create())
session.compile(sources)

result = session.run(transform="orders.transforms:EnrichOrders", orders=orders_df)
```

The session compiles every concrete transform in `sources` and retains those results. Select a compiled transform with
its Python module and class name. See [disk-less source compilation](dev/specifications/DisklessSourceCompilation.md).

Reference: [transforms API](api/Transforms.api.md), [execution](background/Execution.back.md), and
[execution semantic contract](background/ExecutionSemanticContract.back.md).

## Compatibility

Execution and generated-code execution target ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs for
PySpark 3.5.x and 4.0.x by default:

```toml
execution_mode = "online"

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

Spark Connect uses `plugin.pyspark.variant = "spark-connect"`. It is the supported PySpark variant for completed
compiler-visible batch features.
See [Compatibility.md](Compatibility.md).

Local integration lanes cover ordinary PySpark and Spark Connect:

```text
make integration BACKEND=pyspark35
make integration BACKEND=pyspark40
make integration BACKEND=spark-connect35
make integration BACKEND=spark-connect40
```

Reference: [compatibility policy](background/CompatibilityPolicy.back.md) and
[backend capabilities](background/BackendCapabilities.back.md).

## Schema Generation Tool

Generate starter Structure schema classes from live Spark schema metadata:

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
    special,
    step,
    transform,
)

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

Reference: [schemas API](api/Schemas.api.md), [CLI](background/CLI.back.md), and
[schema declaration syntax](reference/Schema.ref.md).

## Next Steps

Code examples: [Examples](../examples/Readme.md)

Get started: [GettingStarted.md](GettingStarted.md)

API: [API.md](API.md)

API catalog: [APICatalog.md](APICatalog.md)

API reference: [API.ref.md](reference/API.ref.md)

Reference docs: [Reference.md](Reference.md)



## Appendix

## Structure additions to PySpark

## Scan

Use `scan(...)` when an output row depends on state produced by earlier rows in the same explicitly ordered partition.
Use `lag(...)` when the previous value already exists in the input relation; `scan(...)` is for feedback recurrence.

```python
class Tick(Schema):
    series = string(nullable=False)
    index = long(nullable=False)


class FibonacciState(Schema):
    previous = long(nullable=False)
    current = long(nullable=False)


class Fibonacci(Schema):
    series = string(nullable=False)
    index = long(nullable=False)
    value = long(nullable=False)


class FibonacciFromTimeline(Transform):
    ticks = input(Tick)
    values = output(Fibonacci)

    def calculate(self, tick: Tick) -> Fibonacci:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=tick.series,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current,
            ),
        )
        return Fibonacci(series=tick.series, index=tick.index, value=state.previous)
```

`scan(...)` returns the state before the transition for the current timeline row. Each partition starts from the same
fully populated `initial` state; empty input returns an empty output relation with the declared schema. The current
release requires nonempty `partition_by` and `order_by`, accepts only ascending order and `"error"`, rejects
null order keys, fails duplicate order keys during Spark evaluation, and enforces a positive literal `max_rows` per
partition.

The PySpark target lowers the recurrence through public DataFrame and Column APIs: group by partition keys, collect and
sort the payload timeline, fold it with higher-order `aggregate(...)`, then expand one output row per input row. It is
batch-only and ordinary-PySpark-only; it does not use UDFs, Pandas, RDDs, Spark actions, driver loops, streaming state; nor it persists the state between transform runs.

Reference: [Ordered Timeline Scan](dev/specifications/OrderedTimelineScan.md),
[API extensions](APIExtensions.md), [IR](background/PySparkCodeGeneration.back.md), and
[PySpark code generation](background/PySparkCodeGeneration.back.md).
