# Quick Reference

## Schema Classes

A schema class defines a row contract and compiles into PySpark schema (`StructType`/`StructField`).

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    total = field(String(), nullable=True)

class OrderNormalized(OrderRaw):
    pass

class OrderWithCustomer(OrderRaw):
    customer_name = field(String(), nullable=True)
```

Use schema classes for inputs, intermediate rows, and outputs. 

Inheritance allows to for schema reuse, while avoiding repeat declarations.

Use `alias=` when the Spark DataFrame column is not a Python identifier.

Reference: [schema declaration syntax](reference/SchemaDeclarationSyntax.md),
[schema semantics](reference/SchemaSemantics.md), and
[nullability and type coercion](reference/NullabilityAndTypeCoercion.md).

## Transform Classes

A transform class is declared by inheriting `Transform`.

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        return OrderNormalized.project(order)(
          total=to_decimal(order.total, precision=12, scale=2))        
```

A 'step method' is transform method that receives and returns a schema class(es), like the `normalize` method above. A transform may have multiple step methods, which are executed in the order of their declaration.

Run the transform:

```python
session = StructureSession(spark=spark)

result = NormalizeOrders(
    orders=orders_df,
).run(session)

normalized_df = result.normalized
```

Invocation of run() method compiles transform and invokes its steps methods. 

Transform classes may inherit from other Transform classes. In such case, parent transforms execute first:

```python
class NormalizeBase(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        ...

class PublishOrders(NormalizeBase):
    published = output(OrderPublished)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        ...
```

Parent transform step methods run before child transform step methods. Multiple parents are allowed: their step methods run in the declared order, left to right. A step method override in child transform can call parent implementation: `super().normalize(order)`, `Base.normalize(self, order)`, or
`super(Base, self).normalize(order)`.

Step methods do not call other step methods directly; attempt to do so, except for the override case above, will result in error. 

Reference: [DSL](reference/DSL.md), [online execution](reference/OnlineExecution.md), and
[transform inheritance and composition](reference/TransformComposition.md).

## Inputs

Inputs are named class attributes that correspond to PySpark DataFrames

```python
orders = input(OrderRaw)
customers = input(Customer)
products = input(Product)
```

When more than one input exists of same schema, the step method must disambiguate with @transform(input) decoration:

```python
orders_external = input(OrderRaw)
orders_internal = input(OrderRaw)

@transform(input=orders_external)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Reference: [DSL inputs](reference/DSL.md) and
[source module rules](reference/SourceModuleRules.md).

## Lanes

Intermediate lanes can be declared to for funnel
stages and branching. Most transforms don't need lanes.

```python
orders_raw = input(OrderRaw)
orders_normalized = lane(OrderNormalized)
orders_rejected = lane(OrderRaw)
published = output(OrderEnriched)
```

The compiler infers input and lane sources from parameter types. If that can't be done (as with `orders_raw`/`orders_rejected` above), disambiguate using @transform decoration:

```python
@transform(output=orders_rejected)
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
@transform(output=[orders_with_product, orders_audited])
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

The returned values are mapped to transform's outputs/lanes, based on schema class. Use @transform decoration with `input=`/`output=` to disambiguate.

In `input=`/`output=`  values, a same-named lane with matching schema wins over the original input. Role selectors like `lane()`, `input()`, `output()` can be used to further disambiguate, if that matters:

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

Reference: [DSL subtransforms](reference/DSL.md),
[symbolic execution](reference/SymbolicExecution.md), and
[execution semantics](reference/ExecutionSemanticContract.md).

## Online Execution

Structure does not own storage orchestration. Callers own `write`, `writeStream`, table creation,
partitioning, checkpoints, output modes, and storage options.

Construct a transform object specifying applicable inputs. Running it triggers in-memory compilation and executes the compiled code.

```python
from structure import StructureConfig, StructureSession

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

Reference: [online execution](reference/OnlineExecution.md) and
[execution semantic contract](reference/ExecutionSemanticContract.md).

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

Reference: [PySpark code generation](reference/PySparkCodeGeneration.md).

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

Reference: [DSL filtering](reference/DSL.md) and
[symbolic execution](reference/SymbolicExecution.md).

## Add and Drop Columns

Add columns by returning a schema with more fields. Drop columns by returning a schema with fewer fields.

```python
class OrderWithFlags(OrderWithCustomer):
    is_large = field(Boolean())

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

Reference: [schema semantics](reference/SchemaSemantics.md) and
[PySpark code generation](reference/PySparkCodeGeneration.md).

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

Reference: [DSL expressions](reference/DSL.md) and
[nullability and type coercion](reference/NullabilityAndTypeCoercion.md).

## Expression Methods

A Transform class can declare expression methods for reusable expressions. Expression methods are expected to have compileable code, and Structure will fail if it can't compile. Use optional decoration `@special(type="expr")` if demarcation is needed for clarity.

```python
@special(type="expr")
def clean_id(value):
    return lower(trim(value))
```

Expression methods do not take `self`, but can be called through `self`.

```python
customer_id=self.clean_id(order.customer_id)
```

Reference: [DSL expression helpers](reference/DSL.md).

## Aggregations

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

Core aggregate helpers are `count()`, `count_distinct(...)`, `sum(...)`, `min(...)`, `max(...)`, and `avg(...)`.
Advanced helpers include `bool_and(...)`, `bool_or(...)`, `stddev(...)`, `variance(...)`, `corr(...)`, `covar(...)`,
`approx_count_distinct(...)`, `approx_percentile(...)`, `collect_list(...)`, `collect_set(...)`, `first_value(...)`,
and `last_value(...)`. Aggregate helpers accept `where=...` for metric-local filters. Post-aggregate `having(...)` and
arbitrary `grouping_sets(...)` are reserved capability boundaries.

Use `rollup(...)` for hierarchical subtotals and `cube(...)` for all grouping-key combinations.
Subtotal rows may omit some grouping keys, so nullable subtotal fields or explicit labels are required.

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
return cube(
    tenant_id=order.tenant.tenant_id,
    product_category=order.product_category,
    customer_tier=order.customer_tier,
).agg(
    grouping_id=grouping_id(),
    order_count=count(),
    distinct_customers=count_distinct(order.customer_id),
    gross_total=sum(order.total),
).as_schema(OrderProductCube)
```

Reference: [advanced analytical operations](reference/AdvancedAnalyticalOperations.md), [DSL](reference/DSL.md),
[IR](reference/IntermediateRepresentation.md), [PySpark code generation](reference/PySparkCodeGeneration.md), and
[streaming compatibility](reference/StreamingCompatibility.md).

## Latest and Earliest Rows

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
required so the selection is reviewable, and the current public tie policy is `TiePolicy.ERROR`. 

Streaming: Selected-row helpers
are batch-only in v2 streaming compatibility checks, because streaming-safe ranking needs explicit watermark and state
semantics (planned).

Reference: [DSL](reference/DSL.md), [IR](reference/IntermediateRepresentation.md),
[PySpark code generation](reference/PySparkCodeGeneration.md), and
[streaming compatibility](reference/StreamingCompatibility.md).

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

`partition_by` is required and accepts one expression or a list/tuple of expressions. `order_by` is required.
Set `descending=True` when the window order should be descending. `lag(...)` and `lead(...)` default to offset `1`;
pass `offset=...` and `default=...` when needed. `rolling_sum(...)`, `rolling_avg(...)`, `rolling_min(...)`, and
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
`range_between(preceding(10), current_row())`. 

Streaming: broad window helpers are batch-only in v2 streaming compatibility.

Reference: [advanced analytical operations](reference/AdvancedAnalyticalOperations.md), [DSL](reference/DSL.md),
[IR](reference/IntermediateRepresentation.md), [PySpark code generation](reference/PySparkCodeGeneration.md), and
[streaming compatibility](reference/StreamingCompatibility.md).

## Removing Duplicate Rows

Use `distinct(relation)` for exact duplicate removal over a relation. It is a synonym for
`drop_duplicates(relation)`.

```python
def unique_events(self, event: RawEvent) -> RawEvent:
    distinct(event)
    return RawEvent.project(event)
```

`drop_duplicates(...)` with accepts list of typed field expressions for PySpark-compatible subset dedupe. The relation is
inferred when all fields come from the same relation:

```python
def unique_accounts(self, event: RawEvent) -> RawEvent:
    drop_duplicates(event.account_id)
    return RawEvent.project(event)
```

Dedupe operations run in source order: before a relation is joined they prepare that relation's
source; after a join they apply to the active joined frame using the specified relation fields.

When the selected row must be deterministic, prefer `dedupe_latest_by(...)` or `dedupe_earliest_by(...)` 
with an explicit ordering and tie policy. 

```python
def latest_events(self, event: RawEvent) -> RawEvent:
    dedupe_latest_by(event.sequence, partition_by=event.account_id)
    return RawEvent.project(event)
```

Streaming: exact duplicate removal is batch-only in v2 streaming compatibility because streaming dedupe needs explicit
watermark, state, and output-mode semantics.

Reference: [DSL](reference/DSL.md), [IR](reference/IntermediateRepresentation.md),
[PySpark code generation](reference/PySparkCodeGeneration.md), and
[streaming compatibility](reference/StreamingCompatibility.md).

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
)
```

Callbacks are symbolic: they are evaluated once against a Structure expression, not row-by-row in Python. Callback
bodies must return typed Structure expressions or typed literals. Python boolean control flow such as `tag and ...`
is rejected; combine symbolic predicates with `&`, `|`, and `~`.

Reference: [advanced analytical operations](reference/AdvancedAnalyticalOperations.md), [DSL](reference/DSL.md), and
[backend capabilities](reference/BackendCapabilities.md).

## Joins

Use symbolic joins. Ref: [Join semantics](reference/JoinSemantics.md),
[analytical join coverage](reference/AnalyticalJoinCoverage.md), and
[full PySpark join support](reference/FullPySparkJoinSupport.md).

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

Use `inner_join(...)` when one current row should intentionally produce one output row per right-side match:

```python
inner_join(
    on=(shipment.order_id == order.id),
    strategy=JoinStrategy.SHUFFLE_HASH,
)
```

Use rowset joins when the join can admit right-only rows, left-only rows, or a Cartesian product:

```python
full_join(on=customer.id == order.customer_id)
right_join(on=customer.id == order.customer_id)
cross_join(calendar_day, allow_cartesian=True)
```

`left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and `cross_join(...)` are shortcuts over
`rowset_join(...)`. Predicate shortcuts can be bare when the right relation is unambiguous.

See `examples/orders/transforms/rowset_join.py` for a generated example covering `full_join(...)`, `right_join(...)`,
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

### Schema Inheritance

Schema classes can subclass other schema classes.
This can help to avoid duplicate field declarations, allow for 'declare once' style and establishing schema hierarchies.

When constructing a subclass schema object, use `.base(row)(...)` to copy inherited fields from a base class instance. 

```python
def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
    left_join(
        on=order.customer_id == customer.id,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer.base(order)(
        customer_name=customer.name,
    )
```

Multiple schema bases compose left to right. 
The `.base()` method allows for multiple bases when constructing a derived class instance:

```python
class OrderPublished(OrderPublication, PublicationFlags):
    pass


flags = PublicationFlags(
    has_customer=order.customer_name.is_not_null(),
    has_product=order.product_name.is_not_null(),
)

return OrderPublished.base(order, flags)
```

To copy the fields from an unrelated/non-base class, use `.project()` method.
The method copies same-named fields of compatible type.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized.project(order)(
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

Reference: [schema inheritance](reference/SchemaInheritance.md),
[schema declaration syntax](reference/SchemaDeclarationSyntax.md), and
[schema semantics](reference/SchemaSemantics.md).

### Transform Inheritance

Transform classes can subclass other Transforms. They inherit inputs, lanes, outputs, hooks, helpers, and subtransforms 
from parent class. Parent transforms run before child transform; a child method with the same name overrides
the inherited scheduled step. Multiple inheritance is allowed, in which case parents run left-to-right before
children, and Python rules for resolving diamond inheritance shapes are observed.  

```python
class NormalizeBase(Transform):
    orders = input(OrderRaw)
    normalized = lane(OrderNormalized)

    @transform(output=normalized)
    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(
            id=lower(trim(order.id)),
            customer_id=lower(trim(order.customer_id)),
        )

class PublishOrders(NormalizeBase):
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

Reference: [transform inheritance and composition](reference/TransformComposition.md),
[DSL](reference/DSL.md), and [execution semantics](reference/ExecutionSemanticContract.md).

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

Reference: [hook semantics](reference/HookSemantics.md) and
[validation semantics](reference/ValidationSemantics.md).

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

Reference: [validation semantics](reference/ValidationSemantics.md) and
[data quality constraints](reference/DataQualityConstraints.md).

## Source and Generated Paths

Default filesystem layout:

```text
src/orders/...
generated/structure_generated/orders/...
```

Generated paths are used only when Structure is configured to emit PySpark code; online execution is the
default. These paths are configurable. Mark `src` and `generated` as source roots in the IDE.

Reference: [source module rules](reference/SourceModuleRules.md),
[configuration schema](reference/ConfigSchema.md), and
[PySpark code generation](reference/PySparkCodeGeneration.md).

## Streaming Compatibility

Structure transforms operate on DataFrames. If the input DataFrame is streaming and every compiled operation
is supported by Spark Structured Streaming, the transform can run in a streaming pipeline.

Structure does not generate `readStream` or `writeStream` before v3; the caller owns streaming orchestration.

Reference: [streaming compatibility](reference/StreamingCompatibility.md).

## Compatibility

Online and generated execution target ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs for
PySpark 3.5.x and 4.0.x by default:

```toml
execution_mode = "online"
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "ordinary"
```

Spark Connect uses `target_backend = "pyspark"` with `target_variant = "spark-connect"`. It is the supported PySpark
variant for completed compiler-visible batch features once Sprint 09 runtime and CI evidence is in place.
See [Compatibility.md](Compatibility.md).

Local integration lanes cover ordinary PySpark and Spark Connect:

```text
make integration BACKEND=pyspark35
make integration BACKEND=pyspark40
make integration BACKEND=spark-connect35
make integration BACKEND=spark-connect40
```

Reference: [compatibility policy](reference/CompatibilityPolicy.md) and
[backend capabilities](reference/BackendCapabilities.md).

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

Reference: [CLI](reference/CLI.md) and
[schema declaration syntax](reference/SchemaDeclarationSyntax.md).

## Planned Features

Implemented v2 analytical features include existence joins, `inner_join(...)`, deterministic lookup dedupe,
temporal validity joins, backward as-of joins, aggregation/grouping, latest/earliest selected-row and keyed-dedupe
helpers, exact duplicate-row removal, Spark higher-order array/map helpers, caching, and target capability checks.

Remaining planned v2 features include:

- Broader deduplication helpers.
- Repartition and coalesce annotations.

These features remain explicit because Structure should not hide performance-sensitive choices.

Planned v2 adoption tooling also includes richer explain output, generated documentation artifacts for schemas
and transforms, production incremental compilation, and a pytest helper for compiler checks and generated-code
freshness.

Reference: [analytical join coverage](reference/AnalyticalJoinCoverage.md),
[backend capabilities](reference/BackendCapabilities.md), and
[alternative backends](reference/AlternativeBackends.md).

## Next Steps

Get started: [GettingStarted.md](GettingStarted.md)

Browse deeper behavior definitions: [Reference.md](Reference.md)
