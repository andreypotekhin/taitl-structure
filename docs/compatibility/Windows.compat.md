# Windows Compatibility

This is the compatibility companion to the [API reference](../api/Windows.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Window Functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `row_number(...)` | `row_number` | `row_number(partition_by=order.customer_id, order_by=order.at)` | yes | yes | Non-null Long. |
| `rank(...)` | `rank` | `rank(partition_by=order.customer_id, order_by=order.at)` | yes | yes | Non-null Long with gaps after ties. |
| `dense_rank(...)` | `dense_rank` | `dense_rank(partition_by=order.customer_id, order_by=order.at)` | yes | yes | Non-null Long without gaps after ties. |
| `percent_rank(...)` | `percent_rank` | `percent_rank(over=w)` | yes | yes | Non-null Double. |
| `cume_dist(...)` | `cume_dist` | `cume_dist(over=w)` | yes | yes | Non-null Double. |
| `ntile(...)` | `ntile` | `ntile(4, over=w)` | yes | yes | Positive literal bucket count, non-null Integer. |
| `lag(...)` | `lag` | `lag(order.total, partition_by=order.customer_id, order_by=order.at)` | yes | yes | Status: `caller-owned-guided`. preserves the value type; `default` must be a compatible Python scalar literal. Migration: Use literal defaults with the typed helper; keep expression-valued defaults in native PySpark. |
| `lead(...)` | `lead` | `lead(order.total, partition_by=order.customer_id, order_by=order.at)` | yes | yes | Status: `caller-owned-guided`. preserves the value type; `default` must be a compatible Python scalar literal. Migration: Use literal defaults with the typed helper; keep expression-valued defaults in native PySpark. |
| `first_value(...)` | `first_value` | `first_value(order.id, over=w)` | yes | yes | Status: `caller-owned-guided`. preserves the value type; `ignore_nulls` is a Python Boolean. Migration: Use a Boolean option with the typed helper; keep an expression-valued `ignoreNulls` in native PySpark. |
| `last_value(...)` | `last_value` | `last_value(order.id, over=w)` | yes | yes | Status: `caller-owned-guided`. preserves the value type; `ignore_nulls` is a Python Boolean. Migration: Use a Boolean option with the typed helper; keep an expression-valued `ignoreNulls` in native PySpark. |
| `nth_value(...)` | `nth_value` | `nth_value(order.id, 2, over=w)` | yes | yes | Status: `caller-owned-guided`. requires positive literal `n` and Boolean `ignore_nulls`; result preserves the value type and is nullable. Migration: Use literal options with the typed helper; keep expression-valued `ignoreNulls` in native PySpark. |

## Function-index dispositions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `window(...)` | `window` | `window(o.at, "10 minutes", "5 minutes")` | yes | yes | Builds a typed event-time `TimeWindow`; the keyword-only analytic `window(partition_by=..., order_by=...)` is a separate Structure overload. |
| `window_time(...)` | `window_time` | `window_time(order.value)` | yes | yes | Returns the end-time expression for a typed `TimeWindow`. |

## Window and ranking helpers

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `rolling_sum(...)` | `sum` | `rolling_sum(o.total, partition_by=p, order_by=t, preceding=6)` | yes | yes | Computes a sum over an ordered row frame ending at the current row; partition, order, and frame are explicit. |
| `rolling_avg(...)` | `avg` | `rolling_avg(o.total, partition_by=p, order_by=t, preceding=6)` | yes | yes | Computes an average over an explicit row frame; partition, order, and preceding bound define the window. |
| `rolling_min(...)` | `min` | `rolling_min(o.total, partition_by=p, order_by=t, preceding=6)` | yes | yes | Computes a minimum over an explicit ordered row frame. |
| `rolling_max(...)` | `max` | `rolling_max(o.total, partition_by=p, order_by=t, preceding=6)` | yes | yes | Computes a maximum over an explicit ordered row frame. |
| `rows_between(...)` | `rowsBetween` | `rows_between(preceding(2), current_row())` | yes | yes | Builds an inclusive row-count frame from symbolic start/end bounds for a reusable analytic window. |
| `range_between(...)` | `rangeBetween` | `range_between(preceding(10), current_row())` | yes | yes | Builds a value-range frame; the order expression and bounds follow PySpark Window range semantics. |
| `unbounded_preceding(...)` | `Window.unboundedPreceding` | `rows_between(unbounded_preceding(), current_row())` | yes | yes | Returns the symbolic lower bound that includes all preceding rows in a frame. |
| `unbounded_following(...)` | `Window.unboundedFollowing` | `rows_between(current_row(), unbounded_following())` | yes | yes | Returns the symbolic upper bound that includes all following rows in a frame. |
| `current_row(...)` | `Window.currentRow` | `rows_between(preceding(2), current_row())` | yes | yes | Returns the symbolic current-row frame bound. |
| `preceding(...)` | Frame bound | `preceding(2)` | yes | yes | Use frame constructors with reusable windows; `preceding(...)` and `following(...)` require non-negative values. |
| `following(...)` | Frame bound | `following(2)` | yes | yes | Use frame constructors with reusable windows; `preceding(...)` and `following(...)` require non-negative values. |
| `window_sum(...)` | `sum` | `window_sum(order.total, over=w)` | yes | yes | Applies `sum(...)` over the supplied analytic window and preserves the inferred aggregate type. |
| `window_avg(...)` | `avg` | `window_avg(order.total, over=w)` | yes | yes | Applies `avg(...)` over the supplied analytic window and returns Spark’s nullable average type. |
| `window_min(...)` | `min` | `window_min(order.total, over=w)` | yes | yes | Applies `min(...)` over the supplied analytic window for an orderable input. |
| `window_max(...)` | `max` | `window_max(order.total, over=w)` | yes | yes | Applies `max(...)` over the supplied analytic window for an orderable input. |
| `window_count(...)` | `count` | `window_count(over=w)` | yes | yes | Counts non-null values over the supplied window, or rows when no value expression is provided. |
| `window_bool_and(...)` | `bool_and` | `window_bool_and(order.is_paid, over=w)` | yes | yes | Returns whether all non-null Boolean inputs in the supplied window are true. |
| `window_bool_or(...)` | `bool_or` | `window_bool_or(order.is_overdue, over=w)` | yes | yes | Returns whether any non-null Boolean input in the supplied window is true. |
| `window_stddev(...)` | `stddev` | `window_stddev(order.total, over=w)` | yes | yes | Computes nullable sample standard deviation over the supplied window. |
| `window_variance(...)` | `variance` | `window_variance(order.total, over=w)` | yes | yes | Computes nullable sample variance over the supplied window. |
| `window_collect_list(...)` | `collect_list` | `window_collect_list(order.id, over=w)` | yes | yes | Collects non-null input values into an Array over the supplied window; result order needs explicit ordering. |
| `window_collect_set(...)` | `collect_set` | `window_collect_set(order.product_id, over=w)` | yes | yes | Collects distinct non-null input values into an Array over the supplied window; result order is unspecified. |

