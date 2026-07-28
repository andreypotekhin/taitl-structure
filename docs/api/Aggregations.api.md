# Aggregations API

These helpers create compiler-visible grouped, metric, selected-row, and deduplication operations. Examples abbreviate
the current `order` row scope as `o`.

## Simple Grouping And Metrics

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `group_by(...)` | `groupBy` | `group_by(customer_id=order.customer_id)` |
| `count(...)` | `count` | `count()` |
| `count_distinct(...)` | `count_distinct` | `count_distinct(order.customer_id)` |
| `sum(...)` | `sum` | `sum(order.total)` |
| `min(...)` | `min` | `min(order.total)` |
| `max(...)` | `max` | `max(order.total)` |
| `avg(...)` | `avg` | `avg(order.total)` |

**Details And Differences**

- Named keys in `group_by(...)` determine output names.
- Every metric helper in this section accepts a symbolic metric-local `where=` filter, for example
  `sum(order.total, where=order.is_paid)`.
- `where=` must be Boolean. A filtered min/max/avg/sum/first/last can be null when no row qualifies.
- `sum(...)` widens Integer values to Long, Float values to Double, and Decimal precision by ten digits (capped at 38),
  matching Spark's aggregate result type.
- `avg(...)` returns Double for non-Decimal inputs; Decimal averages grow precision and scale by four digits, each capped
  at 38.

## Subtotals And Aggregate Metadata

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `rollup(...)` | `rollup` | `rollup(order.region, order.day)` |
| `cube(...)` | `cube` | `cube(order.region, order.channel)` |
| `grouping_sets(...)` | Explicit grouping sets | `grouping_sets((order.region,), ())` |
| `grouping_id()` | `grouping_id` | `grouping_id()` |
| `is_grouped(...)` | Grouping metadata | `is_grouped(order.region)` |
| `having(...)` | Post-aggregate filter | `group_by(order.id).having(lambda out: out.n > 1)` |

**Details And Differences**

- `grouping_sets(...)` renders explicit grouped branches and `unionByName`; `grouping_sets(())` is a single global
  aggregate branch.
- `grouping_id()` and `is_grouped(...)` describe subtotal rows, whose grouping fields can be null.
- `having(...)` reads aggregate-output scope rather than the input row. It can be a bare statement after grouping or
  chained from `group_by(...)`, `rollup(...)`, `cube(...)`, or `grouping_sets(...)`.

## Advanced Metrics

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `bool_and(...)` | `bool_and` | `bool_and(order.is_verified)` |
| `bool_or(...)` | `bool_or` | `bool_or(order.is_priority)` |
| `stddev(...)` | `stddev` | `stddev(order.total)` |
| `variance(...)` | `variance` | `variance(order.total)` |
| `corr(...)` | `corr` | `corr(order.price, order.quantity)` |
| `covar(...)` | `covar` | `covar(order.price, order.quantity)` |
| `approx_count_distinct(...)` | `approx_count_distinct` | `approx_count_distinct(o.customer_id, relative_sd=0.05)` |
| `approx_percentile(...)` | `approx_percentile` | `approx_percentile(order.total, 0.5, accuracy=100)` |
| `percentile(...)` | `percentile` | `percentile(order.total, 0.5)` |
| `skewness(...)` | `skewness` | `skewness(order.total)` |
| `kurtosis(...)` | `kurtosis` | `kurtosis(order.total)` |
| `collect_list(...)` | `collect_list` | `collect_list(order.customer_id)` |
| `collect_set(...)` | `collect_set` | `collect_set(order.customer_id)` |
| `first_value(...)` | Ordered first-value aggregate | `first_value(order.id, order_by=order.created_at)` |
| `last_value(...)` | Ordered last-value aggregate | `last_value(order.id, order_by=order.created_at)` |

**Details And Differences**

- Statistical metrics return nullable doubles. Collection order is Spark-dependent.
- `collect_list(...)` and `collect_set(...)` skip null inputs and return an empty non-null array when no values qualify.
- `first_value(...)` and `last_value(...)` aggregate forms require a scalar `order_by=` and currently use
  `"error"`; `ignore_nulls=` is supported only with `over=`.
- A filtered `first_value(...)` or `last_value(...)` masks nonqualifying order keys, so an excluded row cannot become
  the selected minimum or maximum.
- Exact percentiles, aggregate aliases, and more statistics remain future work.

## Selection And Dedupe

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `latest_by(...)` | `row_number` selection | `latest_by(order.at, partition_by=order.customer_id)` |
| `earliest_by(...)` | `row_number` selection | `earliest_by(order.at, partition_by=order.customer_id)` |
| `dedupe_latest_by(...)` | Deterministic dedupe | `dedupe_latest_by(order.at, partition_by=order.customer_id)` |
| `dedupe_earliest_by(...)` | Deterministic dedupe | `dedupe_earliest_by(order.at, partition_by=order.customer_id)` |
| `drop_duplicates(...)` | `dropDuplicates` / `dropDuplicatesWithinWatermark` | `drop_duplicates(order.customer_id)` |
| `drop_duplicates_within_watermark(...)` | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(order.customer_id)` |
| `distinct(...)` | `distinct` | `distinct(order)` |

**Details And Differences**

- Selected-row helpers need explicit partition and scalar ordering expressions.
- `drop_duplicates(...)` accepts a same-scope field subset; `distinct(...)` can use the whole relation. For streaming
  frames it requires a preceding watermark and uses bounded `dropDuplicatesWithinWatermark`; batch frames use normal
  `dropDuplicates`. `drop_duplicates_within_watermark(...)` is the explicit streaming-only spelling.
- Operations apply in source order. See [Transforms reference](../background/DSL.back.md).
