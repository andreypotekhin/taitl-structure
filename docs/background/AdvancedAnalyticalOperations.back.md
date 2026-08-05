# Advanced Analytical Operations

Advanced analytical operations are the broader aggregation, window, and collection-helper features added after the
first v2 analytical slice. They let Structure cover multi-level summaries, explicit window frames, and richer array/map
logic while keeping the work visible to Spark.

See the exhaustive [aggregations](../api/Aggregations.api.md), [windows](../api/Windows.api.md), and
[collections](../api/Collections.api.md) API tables for supported names, parity, and examples.

The governing sources are the
[Advanced Analytical Operations specification](../dev/specifications/AdvancedAnalyticalOperations.md) and
[design](../dev/design/AdvancedAnalyticalOperations.md). The specification contains staged-scope language: its
opening boundary defers explicit grouping-set and post-aggregate lowering, while its later implementation sections,
the capability profile, and the checked tests define those forms. This background follows the implemented, tested
contract and labels genuinely deferred behavior below.

The analytical surface supports common grouped aggregates, custom grouping sets, ranking, lag/lead, rolling row
metrics, deterministic latest/earliest selection, exact/subset duplicate removal, and basic array/map callbacks. This
page describes the admitted surface and the boundaries still enforced by backend capability checks.

## Scope and First-Slice Boundary

This topic owns advanced batch aggregation, metric, window, and higher-order behavior. The first analytical slice
already covers `group_by(...)`, basic aggregates, selected-row helpers, exact/subset dedupe, projection windows, and
basic array/map transforms. This topic adds `rollup(...)`, `cube(...)`, explicit grouping sets, additional metrics,
filtered aggregates, reusable windows and frames, richer collection helpers, diagnostics, capabilities, and parity.

Streaming aggregation and broad analytic windows, automatic cost-based optimization, hidden UDF/RDD/Pandas fallback,
and storage writes remain outside this contract. A feature is admitted only when its source semantics, IR, target
recipe, explain/traceability, diagnostics, and tests agree.


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

Shortcut form:

```python
return rollup(
    tenant_id=order.tenant.tenant_id,
    product_category=order.product_category,
    order_date=order.business.order_date,
).agg(
    grouping_id=grouping_id(),
    category_subtotal=is_grouped(order.product_category),
    order_count=count(),
    quantity_total=sum(order.quantity),
).as_schema(OrderRevenueRollup)
```

The shortcut form is useful when the output schema mostly mirrors grouping keys and metrics. The canonical constructor
form is clearer when the output also includes parent structures, literals, or non-key fields that compile through
Structure's implicit first-value aggregate for grouped parent fields.

Cube form:

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

Grouping-set form:

```python
return grouping_sets(
    (order.region, order.customer_id),
    (order.region,),
    (),
).agg(
    grouping_id=grouping_id(),
    region_subtotal=is_grouped(order.region),
    customer_subtotal=is_grouped(order.customer_id),
    order_count=count(),
    gross_total=sum(order.total),
).as_schema(OrderGroupingSetSummary)
```


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
- `collect_list(value, element_type=None, where=None)`;
- `collect_set(value, element_type=None, where=None)`;
- `first_value(value, order_by=..., where=None, ties="error")`;
- `last_value(value, order_by=..., where=None, ties="error")`.

Aggregate helpers support `where=...` for metric-local filters. Use `having(lambda out: ...)` to filter aggregate
output rows after all metrics and subtotal keys are computed.

Rules:

- `where=...` filters only the metric that owns it. It does not filter grouping keys or other metrics.
- `first_value(...)` and `last_value(...)` are deterministic aggregate helpers only when `order_by=...` is supplied.
- `ties="error"` is the admitted tie policy for deterministic first/last aggregate helpers.
- `collect_list(...)` and `collect_set(...)` produce Spark collection aggregates; element ordering is not guaranteed.
- `element_type=...` is required when Structure cannot infer the collection aggregate element type.
- Approximate metrics stay visibly approximate in generated PySpark.
- `having(...)` predicates can reference grouped keys and aggregate output metrics through the callback argument; input
  row fields are unavailable after aggregation.

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
return group_by(customer_id=order.customer_id).agg(
    order_count=count(),
    gross_total=sum(order.total),
).having(
    lambda total: total.order_count > 1
).as_schema(CustomerOrderSummary)
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


## Reusable Windows

Window support includes reusable specs:

```python
customer_window = window(
    partition_by=event.customer_id,
    order_by=[event.sequence.asc(), event.event_id.asc()],
    frame=rows_between(preceding(6), current_row()),
)
```

Window frames are explicit. Row frames count physical rows. Range frames use values in the ordering column and require
a compatible order type. `order_by` accepts one expression or an ordered list/tuple. Use `asc_nulls_first()`,
`asc_nulls_last()`, `desc_nulls_first()`, or `desc_nulls_last()` when null placement must be deterministic. A range
frame accepts exactly one order key.

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
lowering, explain/traceability, diagnostics, and parity tests agree. Streaming aggregation and broad analytic windows,
automatic cost optimization, hidden UDF/RDD/Pandas fallback, and storage writes remain deferred or rejected.

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
