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
- `order_by=` accepts one scalar expression or an ordered list/tuple of scalar expressions. Order descriptors can set
  direction and null placement.
- `lag(...)` and `lead(...)` default to `offset=1`; use a compatible Python scalar literal with `default=` for an
  explicit fallback, including date and timestamp literals. Expression, collection, and object defaults are unsupported.
- `descending=` requires a Boolean direction flag.

## Rolling Windows

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `rolling_sum(...)` | Bounded `sum` window | `rolling_sum(o.total, partition_by=p, order_by=t, preceding=6)` |
| `rolling_avg(...)` | Bounded `avg` window | `rolling_avg(o.total, partition_by=p, order_by=t, preceding=6)` |
| `rolling_min(...)` | Bounded `min` window | `rolling_min(o.total, partition_by=p, order_by=t, preceding=6)` |
| `rolling_max(...)` | Bounded `max` window | `rolling_max(o.total, partition_by=p, order_by=t, preceding=6)` |

**Details And Differences**

- `preceding=` gives the number of prior rows included with the current row.
- All rolling helpers accept `descending=True` for reverse order; the direction flag must be Boolean.
- Rolling and reusable `sum(...)` / `avg(...)` retain Spark's widened numeric result types, including Decimal precision
  and scale growth.

## Reusable Windows And Frames

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `window(...)` | `Window.partitionBy(...).orderBy(...)` | `w = window(partition_by=o.customer_id, order_by=o.at)` |
| `window(event_time, ...)` | `functions.window` | `window(o.at, "10 minutes", "5 minutes")` |
| `rows_between(...)` | `rowsBetween` | `rows_between(preceding(2), current_row())` |
| `range_between(...)` | `rangeBetween` | `range_between(preceding(10), current_row())` |
| `unbounded_preceding()` | `Window.unboundedPreceding` | `rows_between(unbounded_preceding(), current_row())` |
| `unbounded_following()` | `Window.unboundedFollowing` | `rows_between(current_row(), unbounded_following())` |
| `current_row()` | `Window.currentRow` | `rows_between(preceding(2), current_row())` |
| `preceding(...)` | Frame bound | `preceding(2)` |
| `following(...)` | Frame bound | `following(2)` |

**Details And Differences**

- `window(partition_by=..., order_by=..., frame=...)` returns a Structure `WindowSpec`, not a raw PySpark `WindowSpec`.
- `window(event_time, duration, slide=None, start=None)` returns `Struct[TimeWindow]` for a named grouping key. It
  accepts fixed positive interval strings and is the event-time streaming form; its positional arguments cannot mix
  with the analytical keyword family.
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
| `window_bool_and(...)` | `bool_and` over window | `window_bool_and(order.is_paid, over=w)` |
| `window_bool_or(...)` | `bool_or` over window | `window_bool_or(order.is_overdue, over=w)` |
| `window_stddev(...)` | `stddev` over window | `window_stddev(order.total, over=w)` |
| `window_variance(...)` | `variance` over window | `window_variance(order.total, over=w)` |
| `window_collect_list(...)` | `collect_list` over window | `window_collect_list(order.id, over=w)` |
| `window_collect_set(...)` | `collect_set` over window | `window_collect_set(order.product_id, over=w)` |

**Details And Differences**

- `ntile(...)` needs a positive bucket count; `nth_value(...)` indexes from one.
- `first_value(...)`, `last_value(...)`, and `nth_value(...)` support a Boolean `ignore_nulls=` in reusable-window form.
- Aggregate window helpers require an explicit row or range frame. A bounded range frame requires exactly one numeric
  order key; a fully unbounded range frame permits multiple order keys of any orderable scalar type.
- Window `collect_list(...)` and `collect_set(...)` skip null inputs and return empty non-null arrays for empty frames.
- Value window aggregates other than `count(...)` can be null for an empty frame, even when their input is non-null.
- Raw `Column.over(...)` and raw PySpark `WindowSpec` objects are unsupported. See the
  [Transforms background](../background/Transform.back.md).

`window_count_distinct(...)` is intentionally unsupported because Spark does not permit distinct window aggregates.
Use `window_count(...)` or grouped `count_distinct(...)` instead.
