# Aggregations

Advanced analytical operations are the broader aggregation, window, and collection-helper features added after the
first v2 analytical slice. They let Structure cover multi-level summaries, explicit window frames, and richer array/map
logic while keeping the work visible to Spark.

See the exhaustive [aggregations](../api/Aggregations.api.md), [windows](../api/Windows.api.md), and
[collections](../api/Collections.api.md) API tables for supported names, parity, and examples.

The governing sources are the
[Advanced Analytical Operations specification](../dev/specifications/AdvancedAnalyticalOperations.md) and
[design](../dev/design/AdvancedAnalyticalOperations.md). This background follows the implemented, tested contract and
labels genuinely deferred behavior below.

The analytical surface supports common grouped aggregates, custom grouping sets, ranking, lag/lead, rolling row
metrics, deterministic latest/earliest selection, exact/subset duplicate removal, and basic array/map callbacks. This
page describes the admitted surface and the boundaries still enforced by backend capability checks.

## Choosing an Analytical Shape

Use the smallest operation family that expresses the intended cardinality:

```text
one row per group             -> group_by(...) and aggregate metrics
multiple subtotal levels      -> rollup(...), cube(...), grouping_sets(...)
one value per input row       -> window(...) or inline window helpers
one selected row per key      -> latest_by(...) or earliest_by(...)
one transformed collection    -> arr_* or map_* higher-order helpers
one relation-wide assertion   -> the Relations API, not a scalar aggregate callback
```

Grouped aggregation changes relation cardinality. Window projection preserves the current rows while adding derived
values. Selected-row helpers reduce each partition to one deterministic row. Array and map helpers keep one row and
transform a nested value. These distinctions affect nullability, ordering, streaming compatibility, and which fields
are legal in a later expression.

### Ordinary Grouped Summary

The smallest useful aggregate step names its grouping keys and returns one typed output row per group:

```python
class DailyCustomerSales(Schema):
    customer_id = string(nullable=False)
    business_date = date(nullable=False)
    order_count = long(nullable=False)
    gross_total = decimal(22, 2, nullable=True)


def summarize(self, order: FulfilledOrder) -> DailyCustomerSales:
    group_by(
        customer_id=order.customer_id,
        business_date=order.business_date,
    )
    return DailyCustomerSales(
        customer_id=order.customer_id,
        business_date=order.business_date,
        order_count=count(),
        gross_total=sum(order.total),
    )
```

The output constructor describes the post-aggregate row, not the input row. Input fields that are neither grouping keys
nor aggregate expressions are unavailable as ordinary row-local values after `group_by(...)`.

### Windowed Enrichment

Window expressions add metrics without changing the number of input rows:

```python
def add_customer_rank(self, order: FulfilledOrder) -> RankedOrder:
    customer_window = window(
        partition_by=order.customer_id,
        order_by=[order.business_date.asc(), order.id.asc()],
        frame=rows_between(preceding(6), current_row()),
    )
    return RankedOrder(
        order_id=order.id,
        customer_id=order.customer_id,
        order_rank=row_number(
            partition_by=order.customer_id,
            order_by=order.business_date,
        ),
        seven_order_total=window_sum(order.total, over=customer_window),
    )
```

The partition and order expressions are part of the contract. A window does not imply a global order at the result
boundary, and a frame does not imply a watermark or streaming state policy.

### Nested Collection Transformation

Higher-order helpers transform arrays and maps inside one row. Their callbacks are captured symbolically:

```python
def normalize_attributes(self, product: Product) -> ProductProfile:
    attributes = map_filter(
        map_transform_values(
            product.attributes,
            lambda key, value: lower(trim(value)),
        ),
        lambda key, value: value.is_not_null(),
    )
    return ProductProfile(
        product_id=product.id,
        tags=arr_distinct(
            arr_transform(product.tags, lambda tag: lower(trim(tag)))
        ),
        attributes=attributes,
    )
```

Callbacks run during symbolic compilation against typed placeholders. They never run once per row as ordinary Python
functions, and they may not collect data, access a live Spark object, or return an untyped value.

## Scope and First-Slice Boundary

This topic owns advanced batch aggregation, metric, window, and higher-order behavior. The first analytical slice
already covers `group_by(...)`, basic aggregates, selected-row helpers, exact/subset dedupe, projection windows, and
basic array/map transforms. This topic adds `rollup(...)`, `cube(...)`, explicit grouping sets, additional metrics,
filtered aggregates, reusable windows and frames, richer collection helpers, diagnostics, capabilities, and parity.

Advanced streaming aggregation and broad analytic windows, automatic cost-based optimization, hidden UDF/RDD/Pandas
fallback, and storage writes remain outside this contract. A feature is admitted only when its source semantics, IR,
target recipe, explain/traceability, diagnostics, and tests agree.


## Grouping

Supported grouping entry points:

- `group_by(*keys, **named_keys)` for ordinary grouped aggregates;
- `rollup(*keys, **named_keys)` for hierarchical totals;
- `cube(*keys, **named_keys)` for all combinations of grouping keys;
- `grouping_sets(*levels)` for explicit grouping levels such as `(region, customer)` and `()`;
- `grouping_id()` for the Spark grouping bit mask;

`is_grouped(field)` for whether a key is absent in the current subtotal row.

Grouping expression keys should be named. Subtotal rows may omit some grouping keys, so output fields for those keys
must be nullable or explicitly filled with a literal label. `grouping_sets(...)` accepts one argument per level; use an
empty tuple `()` for the grand total level.

Canonical form:

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
        quantity_total=sum(order.quantity),
    )
```

Cube form:

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

Grouping-set form:

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
    region_subtotal=is_grouped(order.region),
    customer_subtotal=is_grouped(order.customer_id),
    order_count=count(),
    gross_total=sum(order.total),
)
```

### Subtotal Nullability And Grouping Identity

Rollups, cubes, and explicit grouping sets can emit subtotal rows in which a grouping key is absent. The missing key is
not a source null; it represents the subtotal level. Keep the field nullable or replace it with a documented label:

```python
return OrderRevenueRollup(
    tenant_id=order.tenant_id,
    product_category=when(
        is_grouped(order.product_category),
        "<all categories>",
    ).otherwise(order.product_category),
    order_date=order.order_date,
    grouping_id=grouping_id(),
    category_subtotal=is_grouped(order.product_category),
    order_count=count(),
    quantity_total=sum(order.quantity),
)
```

`grouping_id()` is the machine-readable grouping level. `is_grouped(...)` identifies which dimension is absent. Do not
use a null key alone to infer a subtotal when the source field itself is nullable.

### Grouping Order And Output Order

Grouping keys retain source order in the plan and explain output. Named keys determine output field names; positional
keys remain ordered expressions and should be projected explicitly. The generated schema follows the target schema
declaration, not the order in which metrics happen to be written in the constructor.


## Aggregates

Supported exact aggregates:

- `count(where=None)`;
- `count_distinct(value, where=None)`;
- `sum(value, where=None)`;
- `min(value, where=None)`;
- `max(value, where=None)`;
- `avg(value, where=None)`.

Supported advanced aggregates:

- `bool_and(predicate, where=None)`;
- `bool_or(predicate, where=None)`;
- `stddev(value, where=None)`;
- `variance(value, where=None)`;
- `corr(left, right, where=None)`;
- `covar(left, right, where=None)`;
- `approx_count_distinct(value, relative_sd=None, where=None)`;
- `approx_percentile(value, percentage, accuracy=None, where=None)`;
- `collect_list(value, order_by=None, element_type=None, where=None)`;
- `collect_set(value, element_type=None, where=None)`;
- `first_value(value, order_by=..., where=None, ties="error")`;
- `last_value(value, order_by=..., where=None, ties="error")`.

Aggregate helpers support `where=...` for metric-local filters. Use `having(lambda out: ...)` to filter aggregate
output rows after all metrics and subtotal keys are computed.

Rules:

- `where=...` filters only the metric that owns it. It does not filter grouping keys or other metrics.
- `first_value(...)` and `last_value(...)` are deterministic aggregate helpers only when `order_by=...` is supplied.
- `ties="error"` is the admitted tie policy for deterministic first/last aggregate helpers.
- `collect_list(...)` preserves the declared `order_by=` sequence; without it, and for `collect_set(...)`, element
  ordering is not guaranteed.
- `element_type=...` is required when Structure cannot infer the collection aggregate element type.
- Approximate metrics stay visibly approximate in generated PySpark.
- `having(...)` predicates can reference grouped keys and aggregate output metrics through the callback argument; input
  row fields are unavailable after aggregation.

### Aggregate Null And Empty Semantics

Aggregate result nullability follows the target's Spark-compatible contract rather than the input field declaration
alone:

- `count()` and `count_distinct(...)` return non-null counts, including zero for an empty group where a group exists;
- `sum(...)`, `avg(...)`, `min(...)`, `max(...)`, and statistical metrics can be null when no input value qualifies;
- metric-local `where=` can make a result null even when the unfiltered group has rows;
- `collect_list(...)` and `collect_set(...)` return typed collections with their documented empty/null behavior;
- ordered `first_value(...)` and `last_value(...)` remain deterministic only with an explicit order expression;
- approximate metrics retain their approximation and accuracy metadata in IR and explain output.

Use `coalesce(...)` only when replacing a null aggregate with a business-approved value:

```python
return CustomerSales(
    customer_id=order.customer_id,
    gross_total=coalesce(sum(order.total), 0),
    average_order=avg(order.total),
)
```

Do not use `0` to hide the distinction between no qualifying value and a measured zero unless the output contract makes
that distinction intentionally irrelevant.

Metric-local filtering:

```python
return OrderRevenueRollup(
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

Post-aggregate filtering:

```python
group_by(customer_id=order.customer_id)
having(
    lambda total: total.order_count > 1
)

return CustomerOrderSummary(
    customer_id=order.customer_id,
    order_count=count(),
    gross_total=sum(order.total),
)
```

Statistical, approximate, and ordered metrics:

```python
return OrderRevenueRollup(
    quantity_stddev=stddev(order.quantity),
    quantity_variance=variance(order.quantity),
    quantity_price_corr=corr(order.quantity, order.product_list_price),
    quantity_price_covar=covar(order.quantity, order.product_list_price),
    quantity_median=approx_percentile(order.quantity, 0.5, accuracy=100),
    estimated_customers=approx_count_distinct(order.customer_id, relative_sd=0.05),
    first_customer_id=first_value(order.customer_id, order_by=order.quantity),
    last_customer_id=last_value(order.customer_id, order_by=order.quantity),
    customer_ids=collect_set(order.customer_id),
    order_ids=collect_list(order.id),
)
```


## Selected-Row Operations

`latest_by(...)` and `earliest_by(...)` select one row per explicit partition. They are not ordinary aggregate metrics
and are not interchangeable with `max(...)` or `min(...)`: the selected row retains all fields from one winning input
row.

```python
def latest_customer_order(self, order: FulfilledOrder) -> LatestOrder:
    latest_by(
        order.business_date,
        partition_by=order.customer_id,
    )
    return LatestOrder(
        customer_id=order.customer_id,
        order_id=order.id,
        business_date=order.business_date,
        total=order.total,
    )
```

The order expression and partition are required. Ties use the configured public tie policy; the admitted default is
`"error"`. Use an ordering expression that is unique at the chosen partition when the domain needs a stable winner
rather than a tie diagnostic:

```python
latest_by(
    order.business_date,
    partition_by=[order.customer_id, order.product_id],
)
```

The selected-row operation is batch-only until Structure defines a bounded streaming state and watermark contract.
`dedupe_latest_by(...)` and `dedupe_earliest_by(...)` are intent-specific aliases for the same deterministic family.


## Reusable Windows

Window support includes reusable specs:

```python
customer_window = window(
    partition_by=event.customer_id,
    order_by=[event.sequence.asc(), event.event_id.asc()],
    frame=rows_between(preceding(6), current_row()),
)
```

Window frames are explicit. Row frames count physical rows. Bounded range frames use values in one numeric ordering
column; fully unbounded range frames may use multiple compatible scalar order keys. `order_by` accepts one expression
or an ordered list/tuple. Use `asc_nulls_first()`, `asc_nulls_last()`, `desc_nulls_first()`, or `desc_nulls_last()`
when null placement must be deterministic.

Frame helpers:

- `rows_between(start, end)`;
- `range_between(start, end)`;
- `unbounded_preceding()`;
- `unbounded_following()`;
- `current_row()`;
- `preceding(value)`;
- `following(value)`.

Supported reusable-window expressions:

- `percent_rank(over=...)`;
- `cume_dist(over=...)`;
- `ntile(value, over=...)`;
- `first_value(value, over=..., ignore_nulls=False)`;
- `last_value(value, over=..., ignore_nulls=False)`;
- `nth_value(value, n, over=..., ignore_nulls=False)`;
- `window_sum(value, over=...)`;
- `window_avg(value, over=...)`;
- `window_min(value, over=...)`;
- `window_max(value, over=...)`;
- `window_count(value=None, over=...)`;
- `window_bool_and(value, over=...)` and `window_bool_or(value, over=...)`;
- `window_stddev(value, over=...)` and `window_variance(value, over=...)`;
- `window_collect_list(value, over=...)` and `window_collect_set(value, over=...)`.

Aggregate window helpers require an explicit `rows_between(...)` or `range_between(...)` frame. Spark does not support
distinct window aggregates, so use grouped `count_distinct(...)` for one summary row per group rather than a
row-preserving window result.

First-slice inline window helpers remain supported when a reusable `window(...)` object is not needed:

- `row_number(partition_by=..., order_by=..., descending=False)`;
- `rank(partition_by=..., order_by=..., descending=False)`;
- `dense_rank(partition_by=..., order_by=..., descending=False)`;
- `lag(value, partition_by=..., order_by=..., offset=1, default=None, descending=False)`;
- `lead(value, partition_by=..., order_by=..., offset=1, default=None, descending=False)`;
- `rolling_sum(value, partition_by=..., order_by=..., preceding=..., descending=False)`;
- `rolling_avg(value, partition_by=..., order_by=..., preceding=..., descending=False)`;
- `rolling_min(value, partition_by=..., order_by=..., preceding=..., descending=False)`;
- `rolling_max(value, partition_by=..., order_by=..., preceding=..., descending=False)`.

Example:

```python
customer_window = window(
    partition_by=order.customer_id,
    order_by=order.quantity,
    frame=rows_between(preceding(2), current_row()),
)

return OrderCustomerWindow(
    order_id=order.id,
    percent_rank=percent_rank(over=customer_window),
    quantity_tile=ntile(2, over=customer_window),
    second_order_id=nth_value(order.id, 2, over=customer_window),
    running_units=window_sum(order.quantity, over=customer_window),
    running_avg_units=window_avg(order.quantity, over=customer_window),
)
```

Range frame example:

```python
amount_window = window(
    partition_by=order.customer_id,
    order_by=order.quantity,
    frame=range_between(preceding(10), current_row()),
)

return OrderCustomerWindow(
    running_units=window_sum(order.quantity, over=amount_window),
)
```

Broad window features remain batch-only until Structure has explicit streaming watermark and state semantics.

Reusable windows can mix distribution, value, and aggregate window helpers in one projection:

```python
return OrderCustomerWindow(
    percent_rank=percent_rank(over=customer_window),
    cume_dist=cume_dist(over=customer_window),
    quantity_tile=ntile(2, over=customer_window),
    first_order_id=first_value(order.id, over=customer_window),
    last_order_id=last_value(order.id, over=customer_window),
    second_order_id=nth_value(order.id, 2, over=customer_window),
    running_units=window_sum(order.quantity, over=customer_window),
    running_avg_units=window_avg(order.quantity, over=customer_window),
    running_min_units=window_min(order.quantity, over=customer_window),
    running_max_units=window_max(order.quantity, over=customer_window),
    running_order_count=window_count(over=customer_window),
)
```


## Higher-Order Helpers

Supported array helpers:

- `arr_transform(value, lambda item: ...)`;
- `arr_filter(value, lambda item: predicate)`;
- `arr_exists(value, lambda item: predicate)`;
- `arr_forall(value, lambda item: predicate)`;
- `arr_zip_with(left, right, lambda left_item, right_item: ...)`;
- `arr_aggregate(value, initial, lambda acc, item: ..., finish=None)`;
- `arr_sort_by(value, lambda item: key, descending=False)`;
- `arr_flatten(value)`;
- `arr_distinct(value)`;
- `arr_position(value, item)`.
- `size(value)`;
- `array_contains(value, item)`;
- `array(*values)`;
- `array_repeat(value, count)`;
- `array_union(left, right)` and `array_except(left, right)`;
- `element_at(value, key)` and `try_element_at(value, key)`.

Supported map helpers:

- `map_transform_values(value, lambda key, value: ...)`;
- `map_filter(value, lambda key, value: predicate)`;
- `map_transform_keys(value, lambda key, value: ..., duplicates="error")`;
- `map_zip_with(left, right, lambda key, left_value, right_value: ...)`;
- `map_keys(value)`;
- `map_values(value)`;
- `map_entries(value)`;
- `map_from_entries(value)`.
- `map_contains_key(value, key)`;
- `map_concat(*values, duplicates="error")`.

Callbacks are symbolic. They run once during compilation against typed placeholder expressions and must return typed
Structure expressions or typed literals. They do not run row by row in Python.

Valid callback style:

```python
arr_exists(order.tags, lambda tag: lower(trim(tag)) == "priority")
```

Unsupported callback style:

```python
arr_exists(order.tags, lambda tag: bool(tag))
```

Use symbolic predicates with `&`, `|`, `~`, and `when(...)`. Python boolean control flow, loops, mutation, live Spark
objects, DataFrames, and runtime sessions are not valid inside compiled callbacks.

Array examples:

```python
trimmed_tags = arr_transform(order.tags, lambda tag: lower(trim(tag)))
priority_tags = arr_filter(trimmed_tags, lambda tag: tag == "priority")
paired_tags = arr_zip_with(order.tags, order.tags, lambda left, right: lower(trim(left)))

return OrderCollectionProfile(
    normalized_tags=arr_distinct(priority_tags),
    has_priority=arr_exists(order.tags, lambda tag: lower(trim(tag)) == "priority"),
    all_tags_present=arr_forall(order.tags, lambda tag: tag.is_not_null()),
    sorted_tags=arr_sort_by(paired_tags, lambda tag: tag),
    flat_tags=arr_flatten(order.nested_tags),
    score_total=arr_aggregate(order.scores, 0, lambda acc, item: acc + item),
    tag_position=arr_position(order.tags, "priority"),
)
```

Map examples:

```python
normalized_attributes = map_filter(
    map_transform_keys(
        map_transform_values(order.attributes, lambda key, value: lower(trim(value))),
        lambda key, value: lower(trim(key)),
    ),
    lambda key, value: value.is_not_null(),
)
entries = map_entries(order.attributes)

return OrderCollectionProfile(
    normalized_attributes=normalized_attributes,
    zipped_attributes=map_zip_with(order.attributes, order.attributes, lambda key, left, right: lower(trim(left))),
    attribute_keys=map_keys(order.attributes),
    attribute_values=map_values(order.attributes),
    roundtrip_attributes=map_from_entries(entries),
)
```

Rules:

- Array predicate callbacks must return Boolean expressions for `arr_filter(...)`, `arr_exists(...)`, and
  `arr_forall(...)`.
- Map predicate callbacks must return Boolean expressions for `map_filter(...)`.
- `map_transform_keys(...)` currently admits `duplicates="error"` only.
- `arr_sort_by(...)` validates the symbolic callback and lowers to Spark-visible array sorting.
- `map_entries(...)` and `map_from_entries(...)` are useful for round-tripping maps through Spark-visible entry arrays.
- `size(...)` accepts Arrays and Maps. `array_contains(...)` and `map_contains_key(...)` preserve Spark's nullable
  collection semantics.
- `array(...)` rejects empty and null-only construction. It widens compatible integral and floating values, but rejects
  incompatible element types before Spark runs.
- Array positions are one-based. Use `try_element_at(...)` where an out-of-range array position should produce null;
  ordinary `element_at(...)` follows Spark's ANSI behavior. Map lookup results remain nullable for absent keys.
- `map_concat(...)` admits only `duplicates="error"`. Keep Spark's `spark.sql.mapKeyDedupPolicy=EXCEPTION` default so
  conflicting runtime keys fail instead of selecting an implementation-dependent value.

Array and map helpers can be mixed in one projection:

```python
normalized_attributes = map_filter(
    map_transform_keys(
        map_transform_values(order.attributes, lambda key, value: lower(trim(value))),
        lambda key, value: lower(trim(key)),
    ),
    lambda key, value: value.is_not_null(),
)

return OrderCollectionProfile(
    normalized_tags=arr_distinct(arr_filter(order.tags, lambda tag: tag.is_not_null())),
    has_priority=arr_exists(order.tags, lambda tag: lower(trim(tag)) == "priority"),
    all_tags_present=arr_forall(order.tags, lambda tag: tag.is_not_null()),
    normalized_attributes=normalized_attributes,
    attribute_keys=map_keys(normalized_attributes),
    attribute_values=map_values(normalized_attributes),
    roundtrip_attributes=map_from_entries(map_entries(normalized_attributes)),
)
```


## Worked Analytical Pipeline

An analytical transform commonly separates relation shaping, aggregation, and row-preserving ranking into typed
intermediate schemas:

```python
class DailyCustomerTotals(Schema):
    customer_id = string(nullable=False)
    business_date = date(nullable=False)
    order_count = long(nullable=False)
    gross_total = decimal(22, 2, nullable=True)


class RankedCustomerDay(Schema):
    customer_id = string(nullable=False)
    business_date = date(nullable=False)
    gross_total = decimal(22, 2, nullable=True)
    day_rank = long(nullable=False)


class CustomerAnalytics(Transform):
    orders = input(FulfilledOrder)
    daily = lane(DailyCustomerTotals)
    ranked = output(RankedCustomerDay)

    @step(output=daily)
    def summarize(self, order: FulfilledOrder) -> DailyCustomerTotals:
        group_by(
            customer_id=order.customer_id,
            business_date=order.business_date,
        )
        return DailyCustomerTotals(
            customer_id=order.customer_id,
            business_date=order.business_date,
            order_count=count(),
            gross_total=sum(order.total),
        )

    @step(output=ranked)
    def rank_days(self, day: DailyCustomerTotals) -> RankedCustomerDay:
        return RankedCustomerDay(
            customer_id=day.customer_id,
            business_date=day.business_date,
            gross_total=day.gross_total,
            day_rank=rank(
                partition_by=day.customer_id,
                order_by=day.gross_total,
                descending=True,
            ),
        )
```

The first step changes cardinality and establishes the daily schema. The second step preserves one row per daily total
while adding a rank. Intermediate schema validation can catch an aggregate type or nullability mismatch before the
window step runs.

### One Step With Multiple Metrics

Metrics can share one grouping operation while retaining separate filters and types:

```python
def summarize_product(self, order: FulfilledOrder) -> ProductSummary:
    group_by(product_id=order.product_id)
    return ProductSummary(
        product_id=order.product_id,
        orders=count(),
        paid_orders=count(where=order.is_paid),
        units=sum(order.quantity),
        average_price=avg(order.unit_price),
        customers=count_distinct(order.customer_id),
        high_value_orders=count(where=order.total >= 1000),
    )
```

Metric-local filters do not remove rows from another metric. If a metric has no qualifying values, its nullable result
remains distinct from a measured zero unless the output explicitly uses `coalesce(...)`.

### Aggregate Then Join

Aggregate results can be joined like any other typed relation. Keep the aggregate boundary explicit so a later join does
not accidentally multiply the input rows before the metric is computed:

```python
def publish_customer_metrics(
    self, day: DailyCustomerTotals, target: CustomerTarget
) -> CustomerMetric:
    left_join(
        target,
        on=(target.customer_id == day.customer_id)
        & (target.business_date == day.business_date),
    )
    return CustomerMetric(
        customer_id=day.customer_id,
        business_date=day.business_date,
        gross_total=day.gross_total,
        target_total=target.target_total,
        target_attained=(day.gross_total >= target.target_total),
    )
```

If the target relation can contain duplicates, select-one or dedupe it before this step. Aggregation does not establish
uniqueness for a later join, and a schema does not declare that proof implicitly.


## Diagnostics And Boundaries

Analytical diagnostics identify the transform, step, operation family, grouping or window scope, source location,
selected target, and shortest correction. They should distinguish a type error from an unsupported capability and from a
business-level null or tie condition.

```text
CompileError: Aggregate input is not numeric

Step:
  CustomerAnalytics.summarize_product

Expression:
  sum(order.status)

Use:
  aggregate a numeric field, or convert the source explicitly before summing.

See docs/api/Aggregations.api.md
```

```text
CompileError: Window frame is incomplete

Expression:
  window_sum(order.total, over=customer_window)

Problem:
  Aggregate window helpers require an explicit rows or range frame.

Use:
  frame=rows_between(preceding(6), current_row())

See docs/api/Windows.api.md
```

```text
CompileError: Symbolic callback required

Expression:
  arr_exists(order.tags, lambda tag: bool(tag))

Use:
  return a symbolic Boolean expression such as `tag == "priority"` or `tag.is_not_null()`.

See docs/api/Collections.api.md
```

Tie failures, approximate metric behavior, empty-frame nullability, and streaming incompatibility should remain
visible in diagnostics and explain output. Structure must not silently select an arbitrary row, silently collect data,
or fall back to a Python UDF when an analytical contract is missing.


## IR, Capabilities, and Optimization Boundary

Analytical operations remain compiler-visible. Their IR records grouping kind and ordered levels, metric inputs and
filters, `having(...)` dependencies, window partition/order/frame expressions, callback placeholders and bodies,
cardinality, streaming classification, source anchors, and capability requirements. The online runner and generated
emitter consume the same lowered recipes; neither reinterprets an aggregate or window while rendering.

The relevant capability names include `aggregate.grouping_sets`, `aggregate.having`, `aggregate.window`,
`higher_order.array`, `higher_order.map`, `window.frame`, and `streaming.aggregate`. Unsupported requirements fail
before execution or generated rendering with the selected target and the shortest supported alternative.

Explicit optimization controls such as cache/persist hints, repartition requests, checkpoint boundaries, and join
strategy hints are separate compiler-visible operations. Structure never introduces caching, cost-based rewrites,
repartitioning, or materialization implicitly. Storage writes and Spark job lifecycle remain caller-owned.

Advanced analytical operations are accepted only when source semantics, IR, capability checks, execution and generated
lowering, explain/traceability, diagnostics, and parity tests agree. Advanced streaming aggregation and broad analytic
windows, automatic cost optimization, hidden UDF/RDD/Pandas fallback, and storage writes remain deferred or rejected.

See also: [Transform](Transform.back.md), [Compiler](Compiler.back.md), [PySpark code generation](Generation.back.md),
[Streaming](Streaming.back.md), and [Capabilities](Capabilities.back.md).

## Compatibility

Advanced analytical helpers depend on backend support. When a configured PySpark profile or Spark Connect variant
cannot support a helper, Structure should fail during compilation or generation with a backend capability diagnostic
instead of producing generated code that will fail later.

Streaming compatibility is conservative. Advanced grouping and broad windows are batch-only in v2. Row-preserving HOFs
may become streaming-compatible only when target evidence and tests prove the specific helper shape.


## Explain Output

Compact explain should summarize advanced operations without overwhelming routine output:

```text
aggregate(aggregate keys=region,customer_id
  levels=region+customer_id|region|() metrics=count streaming_modes=update|complete)
window(percent_rank, partitions=2, order=2, frame=rows)
hof(arr_zip_with, callback=symbolic)
```

Expanded explain should show grouping levels, metric inputs, filter predicates, window partition/order/frame
dependencies, and higher-order callback lineage.
