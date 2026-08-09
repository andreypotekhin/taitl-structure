# Aggregations Reference

Use these operations to collapse rows into grouped summaries, add metrics without changing row grain, select one row
per key, or transform values inside arrays and maps. The [Aggregations background](../background/Aggregations.back.md)
explains grain, null behavior, and the analytical boundaries. The [Aggregations API](../api/Aggregations.api.md),
[Windows API](../api/Windows.api.md), and [Collections API](../api/Collections.api.md) contain the full inventories.

Examples use the order and product schemas introduced in the [Schema reference](Schema.ref.md). Replace those names
with schemas from your application.

## Choose the shape first

| Question | Shape |
| --- | --- |
| One row per key with metrics? | `group_by(...)` or a subtotal form |
| Keep every input row and add context? | A reusable or inline window |
| Retain one row per key? | `latest_by(...)`, `earliest_by(...)`, or deterministic dedupe |
| Transform nested values? | Array/map higher-order helpers |

Grouped aggregation changes the row grain. A window preserves row identity. Do not substitute one for the other merely
because both can calculate a sum or rank.

```python
from structure import *
from structure.plugin.pyspark import *


# One row per customer.
group_by(customer_id=order.customer_id)
summary = CustomerTotal(total=sum(order.total))

# Every order row, plus the customer's running total.
running = window_sum(
    order.total,
    partition_by=order.customer_id,
    order_by=order.created_at,
)
```

Choose the first form when downstream consumers need a summary grain and the second when they still need individual
order identity.

## Grouping

Use grouping when the result should contain one row for each declared key combination.

```python
class CustomerTotals(Transform):
    orders = input(Order)
    totals = output(CustomerTotal)

    def summarize(self, order: Order) -> CustomerTotal:
        group_by(
            tenant_id=order.tenant_id,
            customer_id=order.customer_id,
        )
        return CustomerTotal(
            order_count=count(),
            gross_total=sum(order.total),
            last_order_at=max(order.created_at),
        )
```

| Operation | Purpose |
| --- | --- |
| `group_by(*keys, **named_keys)` | Ordinary grouped summary |
| `rollup(*keys, **named_keys)` | Hierarchical subtotals |
| `cube(*keys, **named_keys)` | All grouping-key combinations |
| `grouping_sets(*levels)` | Explicit grouping levels, including `()` for a global branch |
| `grouping_id()` | Spark grouping bit mask |
| `is_grouped(value)` | Whether a dimension participates in the current subtotal |
| `having(predicate)` | Filter aggregate output rather than input rows |

Named grouping keys determine output names. `having(...)` reads the aggregate-output scope:

```python
group_by(region=order.region).having(lambda result: result.order_count > 10)
```

Subtotal rows can contain null grouping fields even when the source field is non-null. Use `grouping_id()` or
`is_grouped(...)` before treating such a null as a missing source value.

### Grouping contracts

Grouping keys may be positional or named. Named keys are preferable for public output because they establish the
published field names. The order of keys is retained in the grouping plan and affects subtotal levels.

```python
group_by(
    tenant_id=order.tenant_id,
    business_date=to_date(order.created_at),
)
return DailyTotal(
    order_count=count(),
    total=sum(order.total),
)
```

`rollup(a, b)` produces detail, `a`, and grand-total levels. `cube(a, b)` produces every combination. An explicit
`grouping_sets((a, b), (a,), ())` makes the levels visible and supports a global branch. The grouping metadata fields
are structural; do not use nullness alone to identify a subtotal.

`having(...)` runs against aggregate output scope, not source-row scope. It can be chained from `group_by`, `rollup`,
`cube`, or `grouping_sets`, or used as a following statement when the aggregate output is unambiguous.

## Metrics

Use metric helpers to name the values calculated for each grouped result.

```python
group_by(tenant_id=order.tenant_id, customer_id=order.customer_id)
return CustomerMetrics(
    order_count=count(),
    paid_count=count(where=order.status == "paid"),
    gross_total=sum(order.total),
)
```

Metric helpers describe aggregate output fields; they do not return a Python dictionary or a collected value.

### Common metrics

| Operation | Signature |
| --- | --- |
| `count` | `count(value=None, where=None)` |
| `count_distinct` | `count_distinct(value, where=None)` |
| `sum`, `min`, `max`, `avg` | `(value, where=None)` |
| `first_value`, `last_value` | `(value, order_by=..., where=None, ties="error")` |

Every `where=` is local to its metric. It does not filter grouping keys or neighboring metrics.

```python
return CustomerTotal(
    paid_total=sum(order.total, where=order.is_paid),
    paid_orders=count(where=order.is_paid),
    latest_paid=last_value(order.id, order_by=order.created_at, where=order.is_paid),
)
```

### Statistical and collection metrics

| Family | Operations |
| --- | --- |
| Boolean | `bool_and`, `bool_or` |
| Statistics | `stddev`, `variance`, `corr`, `covar`, `skewness`, `kurtosis` |
| Approximate | `approx_count_distinct`, `approx_percentile`, `percentile` |
| Collections | `collect_list`, `collect_set` |
| Mode and Variant | `mode`, `schema_of_variant_agg` |

Use `order_by=` with `collect_list(...)` when sequence matters. `collect_set(...)` and unordered collections have
Spark-dependent order. Both collection aggregates skip null inputs and return an empty non-null array when no values
qualify. A filtered `first_value(...)` or `last_value(...)` cannot select a row excluded by its own filter.

`sum` and `avg` retain Spark-compatible widening: integral sums widen to Long, Float sums to Double, and Decimal
precision/scale grow within Spark's bounds. Statistical results are nullable doubles. `mode(..., deterministic=True)`
requires grouped keys and uses the lowest orderable candidate for ties. Variant aggregates require a resolved PySpark 4
profile.

```python
return ProductStats(
    average_price=avg(product.price),
    price_stddev=stddev(product.price),
    tags=collect_list(product.tag, order_by=product.tag.asc()),
    categories=collect_set(product.category),
)
```

The ordered list has a reproducible order; the set deliberately has no ordering promise.

### Aggregate result behavior

| Input/condition | Result behavior |
| --- | --- |
| `count()` | Non-null Long row count |
| `count(value)` | Counts non-null values |
| `count_distinct(value)` | Distinct non-null count with Spark null semantics |
| Empty `sum`, `min`, `max`, `avg` | Nullable result when no qualifying value exists |
| `collect_list` / `collect_set` with no values | Empty non-null typed array |
| Statistical metric with insufficient values | Nullable Double |
| Filtered metric with no qualifying rows | Null except for count forms |

`where=` belongs to the metric that declares it:

```python
return PaymentSummary(
    all_orders=count(),
    paid_orders=count(where=order.is_paid),
    paid_total=sum(order.total, where=order.is_paid),
)
```

The metric filter does not remove rows from other metrics or grouping keys. Keep a separate step-level `where(...)`
when the whole aggregate input should be narrowed.

Decimal aggregate widening follows Spark's bounded precision rules. Do not rely on the live data distribution to infer
the result type; the compiler uses the declared input type, explicit conversions, and target profile.

### Determinism and ties

`first_value` and `last_value` aggregate forms require a scalar `order_by=` and use `ties="error"` by default. Their
`ignore_nulls=` option belongs to the reusable-window form, not the grouped aggregate form. A filtered first/last
metric masks excluded order keys, so an excluded row cannot become the selected minimum or maximum.

`collect_list` is ordered only when `order_by=` is supplied. `collect_set` has no order guarantee. `mode` is
deterministic only when `deterministic=True` and the candidate type is orderable on the selected target. Approximate
metrics retain their approximation and accuracy metadata in explain output.

```python
return CustomerSummary(
    latest_order=last_value(order.id, order_by=order.created_at, ties="error"),
    preferred_channel=mode(order.channel, deterministic=True),
)
```

Use a scalar tie-breaker or an explicit tie policy whenever the selected value is part of a published result.

## Select and deduplicate rows

Use selected-row helpers when one complete input row should survive for each partition.

```python
latest_by(order.updated_at, partition_by=order.customer_id)
return CurrentCustomer.base(order)()
```

| Operation | Use |
| --- | --- |
| `latest_by(value, partition_by=...)` | Keep the latest row in each partition |
| `earliest_by(value, partition_by=...)` | Keep the earliest row in each partition |
| `dedupe_latest_by(...)` | Deterministic latest-row deduplication |
| `dedupe_earliest_by(...)` | Deterministic earliest-row deduplication |
| `drop_duplicates(...)` | Remove duplicate keys in batch or bounded streaming |
| `drop_duplicates_within_watermark(...)` | Explicit streaming-only bounded dedupe |
| `distinct(...)` | Remove duplicate complete rows or a declared relation shape |

Selected-row helpers require an explicit partition and scalar ordering expression. Add a tie-breaker when the ordering
field is not unique. In streaming, deduplication requires the documented watermark boundary; `drop_duplicates(...)`
lowers to bounded Spark streaming behavior only when that contract is satisfied.

`latest_by` and `earliest_by` select a row; `dedupe_latest_by` and `dedupe_earliest_by` publish a deterministic
deduplicated relation. They are not interchangeable with `max(...)` or `min(...)`: a scalar aggregate returns a value,
while selected-row operations preserve the row's other fields and its relation grain.

```python
def current_product(self, version: ProductVersion) -> ProductVersion:
    dedupe_latest_by(
        version.valid_from,
        partition_by=(version.tenant_id, version.product_id),
    )
    return ProductVersion.project(version)
```

`distinct(...)` removes duplicate relation rows. `drop_duplicates(...)` accepts a same-scope field subset. In a
streaming frame, it requires a preceding watermark and lowers to bounded `dropDuplicatesWithinWatermark`; in batch it
uses ordinary duplicate removal. `drop_duplicates_within_watermark(...)` makes the streaming-only contract explicit.

## Windows

Use a window when each input row must remain visible while receiving a rank, neighbor value, or rolling metric.

```python
w = window(
    partition_by=order.customer_id,
    order_by=order.created_at,
    frame=rows_between(preceding(6), current_row()),
)

return CustomerEvent.project(order)(
    total=window_sum(order.total, over=w),
    previous_total=lag(order.total, partition_by=order.customer_id, order_by=order.created_at),
)
```

| Family | Operations |
| --- | --- |
| Ranking | `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile` |
| Neighbor values | `lag`, `lead`, `first_value`, `last_value`, `nth_value` |
| Aggregates | `window_sum`, `window_avg`, `window_min`, `window_max`, `window_count` |
| Boolean/statistical | `window_bool_and`, `window_bool_or`, `window_stddev`, `window_variance` |
| Collections | `window_collect_list`, `window_collect_set` |
| Rolling | `rolling_sum`, `rolling_avg`, `rolling_min`, `rolling_max` |

Inline helpers require `partition_by=` and `order_by=`. Reusable windows use `rows_between(...)` or
`range_between(...)` with `unbounded_preceding()`, `unbounded_following()`, `current_row()`, `preceding(...)`, and
`following(...)`. A bounded range frame has one numeric order key; a fully unbounded range frame can use multiple
orderable scalar keys.

`first_value`, `last_value`, and `nth_value` accept `ignore_nulls=` in reusable-window form. Window collection
helpers skip null inputs and return empty non-null arrays for empty frames. Raw PySpark `WindowSpec` and `.over(...)`
are not part of the typed API.

### Window frame rules

`rows_between(start, end)` counts physical ordered rows. `range_between(start, end)` uses the ordered value range.
`preceding(...)` and `following(...)` require non-negative bounds. A bounded range frame requires exactly one numeric
order expression; a fully unbounded range frame can use multiple orderable scalar expressions.

Inline helpers such as `row_number`, `rank`, `lag`, and `lead` require explicit `partition_by` and `order_by`. An
`order_by` list may contain direction and null-placement descriptors. `lag` and `lead` default to offset one and accept
only compatible scalar Python literals for `default=`.

Rolling helpers include the current row and the requested number of preceding rows. Reverse order is explicit through
`descending=True`; the flag must be Boolean. Windowed value aggregates can be null for an empty frame even when their
input field is non-null.

```python
seven_orders = window(
    partition_by=order.customer_id,
    order_by=order.created_at,
    frame=rows_between(preceding(6), current_row()),
)
return OrderWithTrend.project(order)(
    rolling_total=window_sum(order.total, over=seven_orders),
)
```

Use a rows frame for the previous seven ordered records; use a range frame when the business rule is based on elapsed
time or another numeric order value.

## Higher-order collections

Callbacks are captured symbolically once; they do not execute Python code for every row.

| Array operation | Map operation |
| --- | --- |
| `arr_transform`, `arr_filter`, `arr_exists`, `arr_forall` | `map_transform_values`, `map_filter` |
| `arr_zip_with`, `arr_aggregate` | `map_transform_keys`, `map_zip_with` |
| `arr_sort`, `arr_sort_by`, `arr_flatten`, `arr_distinct` | `map_keys`, `map_values`, `map_entries` |
| `array_contains`, `size`, `element_at`, `try_element_at` | `map_contains_key`, `map_concat`, `element_at` |

Predicate callbacks must return symbolic Boolean expressions. Array lookup positions are one-based; use
`try_element_at(...)` for a nullable missing-element result. The binary `arr_transform(...)` and `arr_filter(...)`
callback forms instead receive a zero-based, non-null `long` source index; filtering keeps the original source position.
`map_transform_keys(...)` and `map_concat(...)` currently require
`duplicates="error"`.

Array callbacks have distinct contracts:

| Callback family | Required return |
| --- | --- |
| `arr_transform` | Value with the declared output element type |
| `arr_filter`, `arr_exists`, `arr_forall` | Symbolic Boolean |
| `arr_zip_with` | Value compatible with the result element type |
| `arr_aggregate` merge | Exactly the initial accumulator type |
| `arr_aggregate` finish | Optional final value type |
| `arr_sort_by` | Orderable symbolic key |

`arr_transform(value, lambda item, index: ...)` can return a declared struct such as
`PositionedTag(value=item, ordinal=index)` to produce an ordinal-aware array without changing row cardinality. Nested
indexed callbacks retain distinct lexical bindings, including when both callbacks use the names `item` and `index`.

`arr_exists` and `arr_forall` retain Spark's three-valued predicate behavior when nulls prevent a decisive result.
`arr_aggregate` returns null for a null input array; an empty array returns the initial accumulator unchanged. Typed
generators such as `explode_struct`, `posexplode_struct`, and `inline_struct` require an `array<struct>` expression and
an explicit `as_` Schema. Inner generators can remove null/empty arrays; outer generators preserve the input row with
nullable generated fields.

Map keys and values remain typed. `map_zip_with` requires identical key types rather than applying numeric key
widening. `map_transform_keys` and `map_concat` reject duplicate keys unless the explicit admitted duplicate policy is
used.

```python
return ProductFeatures.project(product)(
    normalized_tags=arr_transform(product.tags, lambda tag: lower(trim(tag))),
    has_required_tag=arr_exists(product.tags, lambda tag: tag == "required"),
    visible_attributes=map_filter(
        product.attributes,
        lambda key, value: key.is_not_null() & value.is_not_null(),
    ),
)
```

The callbacks are symbolic expressions. They describe element-level work for the target engine and do not run as a
Python loop over collected rows.

## Streaming boundary

The grouped and windowed forms that are valid for streaming require the corresponding watermark and output-mode
contract. Event-time tumbling, sliding, and session windows must use the documented event-time field and fixed
positive durations. Global analytic windows and batch selected-row helpers remain batch-only. A watermark does not
make an otherwise unbounded or unsupported aggregation safe.

Callers own streaming sources, sinks, checkpoints, triggers, and output modes. Structure only validates and executes the
admitted transformation. See the [Streaming API](../api/Streaming.api.md) for `window(...)`, `session_window(...)`,
watermark ordering, and bounded deduplication.

```python
@transform(streaming=True)
class DailyOrders(Transform):
    events = input(OrderEvent, streaming=True)
    totals = output(DailyOrderTotal)

    def total(self, event: OrderEvent) -> DailyOrderTotal:
        watermark(event.created_at, delay="2 days")
        group_by(window(event.created_at, "1 day"), tenant_id=event.tenant_id)
        return DailyOrderTotal(order_count=count(), total=sum(event.amount))


result = DailyOrders(events=events_df).run(session)
query = result.totals.writeStream.outputMode("append").start(output_path)
```

The transform declares compatibility; the caller still chooses the source, sink, checkpoint, trigger, and query
lifecycle.

## Before publishing an aggregate

- Choose grouped, windowed, selected-row, or higher-order shape before choosing a helper.
- Name grouping keys and aggregate output fields through the output Schema.
- Use metric-local `where=` only for the metric that declares the filter.
- Add scalar ordering and tie policy wherever the result must be reproducible.
- Distinguish subtotal nulls with `grouping_id()` or `is_grouped(...)`.
- Use explicit frames for reusable window aggregates.
- Keep callback bodies symbolic; move arbitrary target code into an explicit hook.
- Confirm the target and streaming profile before using Variant aggregates or stateful windows.

```python
def summarize(self, order: Order) -> CustomerTotal:
    group_by(customer_id=order.customer_id)
    return CustomerTotal(total=sum(order.total, where=order.status == "paid"))
```

This keeps the metric filter local to the metric and leaves the grouping contract visible to the compiler.

## Common corrections

- Use a grouped operation when the output should have one row per key; use a window when source rows must remain.
- Name aggregate results through the output Schema. Raw aggregate aliases are not supported.
- Add `order_by=` to ordered selection and collection operations when deterministic output matters.
- Distinguish subtotal nulls with grouping metadata.
- Keep streaming dedupe and windows within their watermark and output-mode contract.

```python
# Filter every input row before aggregation.
where(order.status == "paid")
group_by(tenant_id=order.tenant_id)
return PaidTotals(total=sum(order.total))

# Filter only one metric while retaining all rows for other metrics.
group_by(tenant_id=order.tenant_id)
return MixedTotals(
    all_orders=count(),
    paid_orders=count(where=order.status == "paid"),
)
```

## Complete metric inventory

The grouped metric families are compiler-visible and typed:

| Family | Operations |
| --- | --- |
| Counts | `count`, `count_distinct` |
| Numeric summary | `sum`, `min`, `max`, `avg` |
| Boolean | `bool_and`, `bool_or` |
| Distribution | `stddev`, `variance`, `skewness`, `kurtosis` |
| Relationship | `corr`, `covar` |
| Approximate | `approx_count_distinct`, `approx_percentile`, `percentile` |
| Ordered selection | `first_value`, `last_value` |
| Collection | `collect_list`, `collect_set` |
| Variant | `mode`, `schema_of_variant_agg` |

Approximate metrics retain the supplied accuracy or relative-standard-deviation option in the compiled plan. They do
not become exact metrics because a caller omits an option. `schema_of_variant_agg` returns a nullable SQL-format schema
string and requires a Variant expression on a resolved PySpark 4 profile.

```python
return MetricSummary(
    unique_users=count_distinct(event.user_id),
    p95_latency=approx_percentile(event.latency_ms, 0.95),
    values=collect_list(event.value, order_by=event.occurred_at.asc()),
)
```

## Complete window inventory

| Window family | Operations |
| --- | --- |
| Ranking | `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile` |
| Neighbor | `lag`, `lead`, `first_value`, `last_value`, `nth_value` |
| Numeric | `window_sum`, `window_avg`, `window_min`, `window_max`, `window_count` |
| Boolean/statistical | `window_bool_and`, `window_bool_or`, `window_stddev`, `window_variance` |
| Collections | `window_collect_list`, `window_collect_set` |
| Rolling | `rolling_sum`, `rolling_avg`, `rolling_min`, `rolling_max` |

`ntile` requires a positive bucket count. `nth_value` is one-based. `lag` and `lead` accept scalar compatible
defaults, not expression or collection defaults. `window_count_distinct` is intentionally unsupported because Spark
does not permit distinct window aggregates; use grouped `count_distinct` when distinctness is required.

```python
return EventWithRank.project(event)(
    rank=row_number(
        partition_by=event.tenant_id,
        order_by=event.occurred_at.desc(),
    ),
)
```

## Complete collection inventory

Array helpers include `array`, `array_repeat`, `array_union`, `array_except`, `array_intersect`, `slice`, `sequence`,
`arr_append`, `arr_prepend`, `arr_insert`, `arr_remove`, `arr_compact`, `arr_flatten`, `arr_distinct`,
`arr_position`, `size`, `array_contains`, `element_at`, and `try_element_at`. Callback forms include
`arr_transform`, `arr_filter`, `arr_exists`, `arr_forall`, `arr_zip_with`, `arr_aggregate`, `arr_sort`, `arr_sort_by`,
and `arr_reverse`.

Map helpers include `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `map_contains_key`, `map_concat`,
`map_transform_values`, `map_transform_keys`, `map_filter`, and `map_zip_with`.

`array(...)` needs at least one typed value and widens compatible numeric values. Array positions are one-based.
`try_element_at(...)` returns null for a missing or out-of-range item, while ordinary `element_at(...)` retains Spark's
ANSI behavior. `arr_compact(...)` removes null elements and changes the result's element nullability.

```python
return ProductFeatures.project(product)(
    first_tag=try_element_at(product.tags, 1),
    compact_tags=arr_compact(product.tags),
)
```

## Choosing a stable aggregate

When reviewing an aggregate, ask:

1. What is the input grain and what is the output grain?
2. Are grouping keys named and tenant/scoping fields complete?
3. Does a metric-local filter differ from a step-level input filter?
4. What should an empty group, null input, or empty collection produce?
5. Is order or tie behavior explicit wherever a result is selected?
6. Does a window retain row identity where a grouped aggregate would collapse it?
7. Does the target profile admit the operation in batch and streaming modes?

These questions are part of the practical contract because the same helper name can produce a materially different
business result when its grain, null policy, or ordering is implicit.

## Explain and target behavior

Explain output should retain grouping keys, metric-local filters, order expressions, frame bounds, tie policy,
collection ordering, approximation settings, and streaming classification. Generated and online execution consume the
same aggregate recipe; neither path should infer a different empty-group or numeric-widening behavior.

An operation admitted on one PySpark profile may be unavailable on another profile or variant. Check the selected target
before using Variant helpers, newer collection functions, or streaming stateful forms. Unsupported aggregate syntax
fails before execution rather than becoming an implicit Python UDF.

For a stable review, record the input Schema, output Schema, grouping grain, expected null/empty behavior, ordering
policy, and target profile alongside any persisted aggregate artifact.

```python
aggregate = CustomerTotals(orders=orders).compile(project_root=".")
# Review grouping, metric filters, and target before persisting its output.
```

An aggregate is complete only when its value, shape, and boundary are all documented. A correct metric with an
undocumented grain is still an unsafe public result.

```python
plan = CustomerTotals(orders=orders).compile(project_root=".")
result = CustomerTotals(orders=orders_df).run(session)
```

Inspect the compiled plan and target before treating the runtime result as a persisted business metric.

When an aggregate is used as a join input, publish its key grain and null behavior beside the relation so a later join
cannot accidentally multiply or discard summary rows.

Keep the aggregate's source snapshot and effective timestamp with persisted results when late-arriving data can change
the metric; otherwise a numerically identical value may not be reproducible from the same business inputs.

## See also

- [Transform reference](Transform.ref.md)
- [Joins reference](Join.ref.md)
- [Aggregations API](../api/Aggregations.api.md)
- [Aggregations background](../background/Aggregations.back.md)
