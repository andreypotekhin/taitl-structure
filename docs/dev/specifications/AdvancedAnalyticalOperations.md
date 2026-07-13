# Advanced Analytical Operations

## Purpose

This specification defines the remaining aggregation, window, and higher-order function behavior beyond the first
Sprint 08 analytical slice. It is implementation-ready input for Sprint 09 work.

The feature goal is full compiler-visible analytical coverage for mainstream batch pipelines. A supported operation
must compile to IR, pass backend capability checks, lower through shared online/generated PySpark recipes, appear in
traceability and explain output, and reject unsupported forms before runtime.

## Scope

In scope:

- advanced grouping through `rollup(...)` and `cube(...)`;
- additional aggregate metric helpers;
- filtered aggregates;
- reusable `window(...)` specifications and explicit row/range frames;
- ranking, distribution, value, offset, and aggregate window expressions beyond the first slice;
- additional array and map higher-order helpers;
- diagnostics, backend capabilities, streaming classification, tests, examples, and public references.

Out of scope:

- streaming aggregation and streaming window orchestration;
- automatic cost-based optimization;
- explicit `grouping_sets(...)` lowering;
- post-aggregate `having(...)` predicates;
- hidden UDFs, RDD fallback, Pandas UDF fallback, or arbitrary row-wise Python callbacks;
- storage writes, table management, and Spark job lifecycle.

## Relationship To First Slice

The first slice remains valid and stable. It already covers:

- typed `group_by(...)`;
- `count()`, `count_distinct(...)`, `sum(...)`, `min(...)`, `max(...)`, and `avg(...)`;
- selected-row helpers and keyed dedupe shortcuts;
- projection helpers for row number, rank, dense rank, lag, lead, and rolling row metrics;
- exact and subset duplicate removal;
- `arr_transform(...)`, `arr_filter(...)`, `map_transform_values(...)`, and `map_filter(...)`.

This specification adds the remaining public surface. Implementations must preserve first-slice behavior and tests.

## Public Imports

The following new symbols are candidates for export from `structure` when implemented:

```python
import structure
```

Public export is part of implementation acceptance. `grouping_sets(...)` is admitted for explicit grouping levels and
lowers through the shared PySpark recipe layer.

## Advanced Grouping

`rollup(...)` and `cube(...)` are step-method-level operations like `group_by(...)`.

Example:

```python
def revenue_rollup(self, order: OrderFulfillment) -> RevenueRollup:
    rollup(
        tenant_id=order.tenant.tenant_id,
        order_date=order.business.order_date,
        product_id=order.product_id,
    )

    return RevenueRollup(
        tenant_id=order.tenant.tenant_id,
        order_date=order.business.order_date,
        product_id=order.product_id,
        grouping_id=grouping_id(),
        gross_total=sum(order.total),
    )
```

Rules:

- Only one grouping operation is allowed in a step method.
- Grouping operations must appear before aggregate output construction.
- Direct field grouping keys may be positional or named.
- Expression grouping keys must be named so the output schema can refer to them.
- `rollup(...)` preserves the declared key order from most detailed to broadest subtotal.
- `cube(...)` creates subtotal combinations for all declared keys.
- `grouping_sets(...)` accepts one tuple/list per grouping level; use `()` for a grand total level.
- Output fields for grouping keys that may be absent in subtotal rows must be nullable unless the user assigns an
  explicit literal replacement.
- `grouping_id()` returns a non-nullable integer-like expression.
- `is_grouped(field)` returns a non-nullable Boolean expression indicating whether a grouping key is absent in the
  current subtotal level.
- `grouping_sets(...)` emits one grouped branch per level and unions the branches by name.

## Aggregate Metrics

Additional exact metrics:

- `bool_and(predicate)`;
- `bool_or(predicate)`;
- `first_value(value, order_by=..., ties=TiePolicy.ERROR)`;
- `last_value(value, order_by=..., ties=TiePolicy.ERROR)`.

Additional statistical metrics:

- `stddev(value)`;
- `variance(value)`;
- `corr(left, right)`;
- `covar(left, right)`.

Approximate metrics:

- `approx_count_distinct(value, relative_sd=None)`;
- `approx_percentile(value, percentage, accuracy=None)`.

Collection metrics:

- `collect_list(value, element_type=...)`;
- `collect_set(value, element_type=...)`.

Rules:

- Numeric metrics require numeric input expressions.
- Boolean metrics require Boolean input expressions.
- `min(...)`, `max(...)`, `first_value(...)`, and `last_value(...)` require orderable input expressions.
- Unordered first/last aggregates are invalid.
- Approximate metrics must be named as approximate in generated code comments, traceability, and explain output.
- `relative_sd`, `percentage`, and `accuracy` must be literals so capability checks can validate them without Spark.
- Collection metrics produce nullable arrays unless the aggregate function and value expression prove otherwise.
- Collection metrics should emit a warning that element ordering is not guaranteed unless an ordered form is later
  admitted.

## Filtered Aggregates

Aggregate metric helpers may accept `where=predicate`:

```python
paid_total=sum(order.total, where=order.status == "paid")
```

Rules:

- The filter predicate references the pre-aggregate row scope.
- The filter predicate is applied only to the metric that owns it.
- Filtered metrics must not change grouping key membership.
- Unsupported backends reject filtered metrics through backend capability diagnostics before rendering.

## Window Specifications

`window(...)` creates a reusable symbolic window specification:

```python
customer_sequence = window(
    partition_by=event.customer_id,
    order_by=[event.sequence.asc(), event.event_id.asc()],
    frame=rows_between(preceding(6), current_row()),
)
```

Rules:

- `partition_by` is required and accepts one expression or an ordered list/tuple.
- `order_by` is required for ranking, offset, value, and range-frame helpers.
- `order_by` accepts expressions or order descriptors with direction and null ordering.
- `frame` is required for broad aggregate window helpers.
- `rows_between(start, end)` uses physical row offsets.
- `range_between(start, end)` uses order-value ranges and requires exactly one compatible order expression.
- Frame bounds may be unbounded, current row, preceding N, or following N.
- Window specs are immutable and may be reused by multiple projection assignments.

Inline helper arguments remain supported. If a helper receives both `over=window(...)` and inline partition/order/frame
arguments, compilation fails with an ambiguity diagnostic.

## Window Expressions

Additional ranking and distribution helpers:

- `percent_rank(over=...)`;
- `cume_dist(over=...)`;
- `ntile(n, over=...)`.

Additional value helpers:

- `first_value(value, over=..., ignore_nulls=False)`;
- `last_value(value, over=..., ignore_nulls=False)`;
- `nth_value(value, n, over=..., ignore_nulls=False)`.

Window aggregate helpers:

- `window_sum(value, over=...)`;
- `window_avg(value, over=...)`;
- `window_min(value, over=...)`;
- `window_max(value, over=...)`;
- `window_count(value=None, over=...)`;
- `window_count_distinct(value, over=...)` where supported.

Rules:

- Ranking and distribution helpers return non-nullable numeric expressions.
- `ntile(n)` requires a positive integer literal.
- Value and offset helpers return the value expression type and preserve or widen nullability according to defaults
  and `ignore_nulls`.
- `ignore_nulls=True` requires backend support.
- Window aggregate helpers require a frame.
- Window expressions are projection expressions. They do not change row count by themselves.
- If a helper can choose among tied rows, the window order must be deterministic or the helper must expose a tie
  policy.

## Higher-Order Functions

Array helpers:

- `arr_exists(array, lambda item: predicate)`;
- `arr_forall(array, lambda item: predicate)`;
- `arr_zip_with(left, right, lambda left_item, right_item: value)`;
- `arr_aggregate(array, initial, lambda acc, item: acc_value, finish=None)`;
- `arr_sort_by(array, lambda item: key, descending=False)`;
- `arr_flatten(array)`;
- `arr_distinct(array)`;
- `arr_position(array, value)`.

Map helpers:

- `map_transform_keys(map_expr, lambda key, value: new_key, duplicates=...)`;
- `map_zip_with(left, right, lambda key, left_value, right_value: value)`;
- `map_keys(map_expr)`;
- `map_values(map_expr)`;
- `map_entries(map_expr)`;
- `map_from_entries(array_expr)`.

Rules:

- Callback arity is fixed by helper and validated during symbolic compilation.
- Callback placeholders are typed from collection element, key, value, or accumulator types.
- Callback returns must be typed Structure expressions or typed literals.
- Callback bodies may call public DSL helpers and `@special(type="expr")` helpers.
- Callback bodies may close over literals and immutable configuration values.
- Callback bodies must not close over live Spark objects, DataFrames, sessions, mutable containers, or runtime-only
  objects.
- Python boolean control flow is invalid inside callbacks. Use `&`, `|`, `~`, `when(...)`, and DSL helpers.
- Python loops, mutation, `yield`, `await`, arbitrary object construction, and side effects are invalid inside
  callbacks.
- Map key transforms must define a duplicate-key policy before duplicate keys are possible. The first admitted policy
  should be `ERROR`.

## IR Requirements

Add or extend IR nodes for:

- `AggregateOperation`;
- `GroupingOperation`;
- `AggregateMetricExpr`;
- `HavingOperation`;
- `WindowSpecExpr`;
- `WindowExpr`;
- `WindowFrame`;
- `HigherOrderExpr`.

Each node records:

- operation or expression kind;
- source anchor;
- input expressions;
- output type and nullability;
- backend-neutral capability requirement;
- streaming compatibility classification;
- cardinality effect for operations;
- dataflow dependencies for traceability.

## Backend Capability Requirements

Each feature family must have explicit capability names:

- `aggregate.rollup`;
- `aggregate.cube`;
- `aggregate.grouping_sets`;
- `aggregate.filtered_metric`;
- `aggregate.approximate_metric`;
- `aggregate.collection_metric`;
- `window.spec`;
- `window.rows_frame`;
- `window.range_frame`;
- `window.value_function`;
- `window.distribution_function`;
- `window.aggregate_function`;
- `hof.array.exists`;
- `hof.array.zip_with`;
- `hof.array.aggregate`;
- `hof.map.transform_keys`;
- `hof.map.zip_with`.

Unsupported features must fail before online execution or generated rendering with a backend diagnostic that names the
configured backend and the missing capability.

## Streaming Compatibility

All features in this specification are batch-only in v2 unless a narrower rule explicitly proves compatibility.

Reason:

- broad aggregation requires output-mode and state semantics;
- broad windows require watermark and state semantics;
- collection HOFs may be row-preserving, but nested semantics and target support need explicit streaming evidence.

The streaming compatibility report must say whether the issue is aggregation state, window state, target support, or
unknown callback compatibility.

## Explain And Traceability

Compact explain must show:

- grouping kind and metric count;
- window function, partition count, order count, and frame kind;
- HOF helper name and callback symbolic status.

Expanded explain must show:

- grouping levels and subtotal fields;
- metric output field, input fields, filter predicate, type, nullability, and approximate/exact marker;
- having predicates and aggregate dependencies;
- window partition, order, frame, and input dependencies;
- HOF placeholders, callback body, captured literals/helpers, and output expression lineage.

Generated traceability must expose enough structured data for Sprint 10 generated docs.

## Acceptance Criteria

- Each admitted helper has public DSL tests, compileability tests, backend capability tests, generated PySpark
  rendering tests, online recipe tests, and public API snapshot coverage.
- Online and generated execution produce equal rows and schemas for representative fixtures.
- Unsupported forms fail with actionable diagnostics before Spark runtime.
- Public reference docs include examples and limitations.
- `structure explain` compact and expanded modes cover each admitted family.
- `make build` passes after each implementation milestone.
