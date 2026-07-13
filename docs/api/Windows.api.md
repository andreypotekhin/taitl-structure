# Windows API

These helpers construct compiler-visible Spark window expressions. Examples abbreviate the current `order` row scope
as `o`, its customer key as `p`, and its event-time order key as `t`.

## Inline Windows

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `row_number(...)` | `row_number` | `row_number(partition_by=order.customer_id, order_by=order.at)` |
| `rank(...)` | `rank` | `rank(partition_by=order.customer_id, order_by=order.at)` |
| `dense_rank(...)` | `dense_rank` | `dense_rank(partition_by=order.customer_id, order_by=order.at)` |
| `lag(...)` | `lag` | `lag(order.total, partition_by=order.customer_id, order_by=order.at)` |
| `lead(...)` | `lead` | `lead(order.total, partition_by=order.customer_id, order_by=order.at)` |

**Details And Differences**

- All inline helpers require `partition_by=` and `order_by=`.
- `lag(...)` and `lead(...)` default to `offset=1`; use `default=` for an explicit fallback.

## Rolling Windows

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `rolling_sum(...)` | Bounded `sum` window | `rolling_sum(o.total, partition_by=p, order_by=t, preceding=6)` |
| `rolling_avg(...)` | Bounded `avg` window | `rolling_avg(o.total, partition_by=p, order_by=t, preceding=6)` |
| `rolling_min(...)` | Bounded `min` window | `rolling_min(o.total, partition_by=p, order_by=t, preceding=6)` |
| `rolling_max(...)` | Bounded `max` window | `rolling_max(o.total, partition_by=p, order_by=t, preceding=6)` |

**Details And Differences**

- `preceding=` gives the number of prior rows included with the current row.
- All rolling helpers accept `descending=True` for reverse order.

## Reusable Windows And Frames

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `window(...)` | `Window.partitionBy(...).orderBy(...)` | `w = window(partition_by=o.customer_id, order_by=o.at)` |
| `rows_between(...)` | `rowsBetween` | `rows_between(preceding(2), current_row())` |
| `range_between(...)` | `rangeBetween` | `range_between(preceding(10), current_row())` |
| `unbounded_preceding()` | `Window.unboundedPreceding` | `rows_between(unbounded_preceding(), current_row())` |
| `unbounded_following()` | `Window.unboundedFollowing` | `rows_between(current_row(), unbounded_following())` |
| `current_row()` | `Window.currentRow` | `rows_between(preceding(2), current_row())` |
| `preceding(...)` | Frame bound | `preceding(2)` |
| `following(...)` | Frame bound | `following(2)` |

**Details And Differences**

- `window(...)` returns a Structure `WindowSpec`, not a raw PySpark `WindowSpec`.
- Use frame constructors with reusable windows; `preceding(...)` and `following(...)` require non-negative values.

## Reusable-Window Functions

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `percent_rank(...)` | `percent_rank` | `percent_rank(over=w)` |
| `cume_dist(...)` | `cume_dist` | `cume_dist(over=w)` |
| `ntile(...)` | `ntile` | `ntile(4, over=w)` |
| `nth_value(...)` | `nth_value` | `nth_value(order.id, 2, over=w)` |
| `first_value(..., over=w)` | `first_value` | `first_value(order.id, over=w)` |
| `last_value(..., over=w)` | `last_value` | `last_value(order.id, over=w)` |
| `window_sum(...)` | `sum` over window | `window_sum(order.total, over=w)` |
| `window_avg(...)` | `avg` over window | `window_avg(order.total, over=w)` |
| `window_min(...)` | `min` over window | `window_min(order.total, over=w)` |
| `window_max(...)` | `max` over window | `window_max(order.total, over=w)` |
| `window_count(...)` | `count` over window | `window_count(over=w)` |
| `window_count_distinct(...)` | `count_distinct` over window | `window_count_distinct(order.customer_id, over=w)` |

**Details And Differences**

- `ntile(...)` needs a positive bucket count; `nth_value(...)` indexes from one.
- `first_value(...)`, `last_value(...)`, and `nth_value(...)` support `ignore_nulls=` in reusable-window form.
- Raw `Column.over(...)` and raw PySpark `WindowSpec` objects are unsupported. Null/multi-key ordering improvements are
  planned for Sprint 14. See [advanced analytical operations](../reference/AdvancedAnalyticalOperations.md).
