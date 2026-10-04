# Aggregations Compatibility

This is the compatibility companion to the [API reference](../api/Aggregations.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Core aggregate functions

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `avg(...)` | `avg` | `avg(order.total)` | yes | yes | Returns a typed nullable aggregate result. |
| `count(...)` | `count` | `count()` | yes | yes | Counts non-null values, or rows when no value is supplied. |
| `collect_list(...)` | `collect_list` | `collect_list(order.customer_id, order_by=order.created_at)` | yes | yes | Returns a typed array; nulls are skipped and order is defined only when requested. |
| `collect_set(...)` | `collect_set` | `collect_set(order.customer_id)` | yes | yes | Returns a typed de-duplicated array; nulls are skipped and order is unspecified. |
| `max(...)` | `max` | `max(order.total)` | yes | yes | Returns the maximum orderable value. |
| `min(...)` | `min` | `min(order.total)` | yes | yes | Returns the minimum orderable value. |
| `sum(...)` | `sum` | `sum(order.total)` | yes | yes | Returns a typed nullable aggregate result. |
| `schema_of_variant_agg(...)` | `schema_of_variant_agg` | `schema_of_variant_agg(order.payload)` | yes | yes | Aggregates observed Variant schemas into a schema description. |
| `first_value(...)` | `first_value` | `first_value(order.id, order_by=order.created_at)` | yes | yes | Selects the first value under an explicit ordering. |
| `last_value(...)` | `last_value` | `last_value(order.id, order_by=order.created_at)` | yes | yes | Selects the last value under an explicit ordering. |

## Boolean Aggregates

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `bool_and(...)` | `bool_and` | `bool_and(order.is_verified)` | yes | yes | Boolean aggregate |
| `bool_or(...)` | `bool_or` | `bool_or(order.is_priority)` | yes | yes | Boolean aggregate |
| `every(...)` | `every` | `every(order.is_verified)` | yes | yes | Boolean aggregate rendered as `F.every(...)` |

## Grouping, selection, and deduplication

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `grouping(...)` | `grouping` | `grouping(order.region)` | yes | yes | Non-null Integer 1 when the key is omitted from the active subtotal level, otherwise 0. |
| `grouping_id(...)` | `grouping_id` | `grouping_id()` | yes | yes | Non-null Integer bitmask for the active subtotal level. |
| `group_by(...)` | `groupBy` | `group_by(customer_id=order.customer_id)` | yes | yes | Groups by named typed keys; `having(...)` filters aggregate output. |
| `rollup(...)` | `rollup` | `rollup(order.region, order.day)` | yes | yes | Creates hierarchical grouping levels. |
| `cube(...)` | `cube` | `cube(order.region, order.channel)` | yes | yes | Creates multidimensional grouping levels. |
| `grouping_sets(...)` | Explicit grouping sets | `grouping_sets((order.region,), ())` | yes | yes | Declares the grouping sets explicitly. |
| `is_grouped(...)` | Grouping metadata | `is_grouped(order.region)` | yes | yes | Returns a Boolean indicating whether the key is present in the active grouping level. |
| `having(...)` | Post-aggregate filter | `having(order.value)` | yes | yes | Filters after aggregation using the declared output schema. |
| `latest_by(...)` | row_number selection | `latest_by(order.created_at, partition_by=order.customer_id)` | yes | yes | Selects the latest row per key; tie behavior follows the helper contract. |
| `earliest_by(...)` | row_number selection | `earliest_by(order.created_at, partition_by=order.customer_id)` | yes | yes | Selects the earliest row per key; tie behavior follows the helper contract. |
| `dedupe_latest_by(...)` | Deterministic dedupe | `dedupe_latest_by(order.created_at, partition_by=order.customer_id)` | yes | yes | Deduplicates by retaining the latest row under the declared ordering. |
| `dedupe_earliest_by(...)` | Deterministic dedupe | `dedupe_earliest_by(order.created_at, partition_by=order.customer_id)` | yes | yes | Deduplicates by retaining the earliest row under the declared ordering. |
| `drop_duplicates(...)` | dropDuplicates / dropDuplicatesWithinWatermark | `drop_duplicates(order.customer_id)` | yes | yes | Drops duplicate keys; streaming behavior depends on the watermark contract. |
| `drop_duplicates_within_watermark(...)` | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(order.customer_id)` | no | yes | Streaming-only bounded deduplication; requires a preceding watermark. |
| `distinct(...)` | `distinct` | `distinct(order)` | yes | yes | Returns distinct rows. |

## Advanced and Sketch Aggregates

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `hll_sketch_agg(...)` | `hll_sketch_agg` | `hll_sketch_agg(order.customer_id)` | yes | yes | Returns branded HLL state; precision is a compile-time integer literal from 4 through 21. |
| `hll_sketch_estimate(...)` | `hll_sketch_estimate` | `hll_sketch_estimate(order.customer_hll)` | yes | yes | Accepts branded HLL state and returns nullable Long. |
| `hll_union(...)` | `hll_union` | `hll_union(order.left_hll, order.right_hll)` | yes | yes | Accepts branded HLL states; mixed precision requires explicit opt-in and emits `SKETCH-W0802`. |
| `bitmap_bit_position(...)` | `bitmap_bit_position` | `bitmap_bit_position(order.position)` | yes | yes | Maps Integer/Long values to a non-null Long bitmap position. |
| `bitmap_bucket_number(...)` | `bitmap_bucket_number` | `bitmap_bucket_number(order.position)` | yes | yes | Maps Integer/Long values to a non-null Long bucket number. |
| `bitmap_construct_agg(...)` | `bitmap_construct_agg` | `bitmap_construct_agg(order.position)` | yes | yes | Builds branded Bitmap state from Integer/Long positions. |
| `bitmap_or_agg(...)` | `bitmap_or_agg` | `bitmap_or_agg(order.feature_bitmap)` | yes | yes | Unions branded Bitmap state with a grouped aggregate. |
| `bitmap_count(...)` | `bitmap_count` | `bitmap_count(order.feature_bitmap)` | yes | yes | Accepts branded Bitmap state and returns nullable Long. |
| `covar_pop(...)` | `covar_pop` | `covar_pop(order.price, order.quantity)` | yes | yes | Nullable Double population covariance. |
| `covar(...)` | `covar_samp` | `covar(order.price, order.quantity)` | yes | yes | Renders to PySpark `covar_samp`; nullable Double sample covariance. |
| `histogram_numeric(...)` | `histogram_numeric` | `histogram_numeric(order.total, 20, to=LatencyBucket)` | yes | yes | Accepts Integer, Long, Float, and Double values on PySpark 3.5/4.0; Decimal input requires the exact `>=4.0,<4.1` profile because live Spark 3.5.0 execution fails with `ClassCastException`. `n_bins` is a foldable Integer literal from 2 through 2,147,483,647. `to` declares exactly nullable `x` matching the input and nullable Double `y`; the result is a nullable array with nullable elements and fields. |
| `hll_union_agg(...)` | `hll_union_agg` | `hll_union_agg(order.sketch)` | yes | yes | Accepts only branded HLL state and returns nullable HLL with the declared precision. The mixed-precision opt-in warns with `SKETCH-W0802`; Spark may reduce precision. |
| — | `count_min_sketch` | `count_min_sketch(order.amount, 0.1, 0.9, 7)` | yes | yes | Status: `caller-owned-guided`. Count-Min serialized bytes have no branded type, estimator, union operation, or interoperability contract in Structure. Migration: Keep this sketch at a native PySpark boundary and treat its Binary payload as Spark-specific opaque state. |
| `mean(...)` | `mean` | `mean(order.total)` | yes | yes | Same Spark numeric widening as `avg`, preserving PySpark spelling. |
| `some(...)` | `some` | `some(order.is_verified)` | yes | yes | Boolean any-true aggregate with preserved PySpark spelling. |
| `std(...)` | `std` | `std(order.total)` | yes | yes | Nullable Double alias for sample standard deviation, preserving PySpark spelling. |
| `approx_percentile(...)` | `percentile_approx`, `approx_percentile` | `approx_percentile(order.total, 0.5, accuracy=100)` | yes | yes | Renders to `F.percentile_approx`. Supports a literal scalar percentage and accuracy. Status: `caller-owned-guided` for row-valued or array-valued percentage/accuracy. Migration: Keep those dynamic forms in native PySpark. |
| `count_distinct(...)` | `count_distinct` | `count_distinct(order.customer_id)` | yes | yes | Status: `caller-owned-guided`. Returns a non-null Long for one scalar expression; Structure does not yet accept additional columns. Migration: Keep tuple/multi-column distinct counting in native PySpark until its null and schema contract is modeled. |
| `count_if(...)` | `count_if` | `count_if(order.is_paid)` | yes | yes | Accepts a typed Boolean and returns non-null Long. |
| `approx_count_distinct(...)` | `approx_count_distinct` | `approx_count_distinct(order.customer_id, relative_sd=0.05)` | yes | yes | Returns non-null Long; relative standard deviation is a validated literal in `(0, 0.39]`. |
| `median(...)` | `median` | `median(order.total)` | yes | yes | Accepts numeric expressions and returns nullable Double. |
| `percentile(...)` | `percentile` | `percentile(order.total, 0.5)` | yes | yes | Status: `caller-owned-guided` for percentage arrays or row-valued percentage/frequency. Returns nullable Double for one literal scalar percentage and positive integer frequency. Migration: Use native PySpark for percentage arrays or row-valued percentage/frequency. |
| `stddev(...)` | `stddev` | `stddev(order.total)` | yes | yes | Returns nullable Double sample standard deviation. |
| `stddev_pop(...)` | `stddev_pop` | `stddev_pop(order.total)` | yes | yes | Returns nullable Double population standard deviation. |
| `stddev_samp(...)` | `stddev_samp` | `stddev_samp(order.total)` | yes | yes | Returns nullable Double sample standard deviation. |
| `variance(...)` | `variance` | `variance(order.total)` | yes | yes | Returns nullable Double sample variance. |
| `var_pop(...)` | `var_pop` | `var_pop(order.total)` | yes | yes | Returns nullable Double population variance. |
| `var_samp(...)` | `var_samp` | `var_samp(order.total)` | yes | yes | Returns nullable Double sample variance. |
| `corr(...)` | `corr` | `corr(order.price, order.quantity)` | yes | yes | Accepts numeric pairs and returns nullable Double Pearson correlation. |
| `skewness(...)` | `skewness` | `skewness(order.total)` | yes | yes | Returns nullable Double skewness. |
| `kurtosis(...)` | `kurtosis` | `kurtosis(order.total)` | yes | yes | Returns nullable Double kurtosis. |
| `mode(...)` | `mode` | `mode(order.category, deterministic=True)` | yes | yes | Preserves Spark's nullable input type and arbitrary tie choice by default; optional deterministic ties choose the lowest orderable value. |
| `any_value(...)` | `any_value` | `any_value(order.category, ignore_nulls=True)` | yes | yes | Preserves input type and has nullable result. |
| `array_agg(...)` | `array_agg` | `array_agg(order.customer_id)` | yes | yes | Returns a typed Array and omits null candidates like Spark's aggregate collection behavior. |
| `bit_and(...)` | `bit_and` | `bit_and(order.flags)` | yes | yes | Accepts Integer/Long and returns nullable Long. |
| `bit_or(...)` | `bit_or` | `bit_or(order.flags)` | yes | yes | Accepts Integer/Long and returns nullable Long. |
| `bit_xor(...)` | `bit_xor` | `bit_xor(order.flags)` | yes | yes | Accepts Integer/Long and returns nullable Long. |
| `first(...)` | `first` | `first(order.category, ignore_nulls=True)` | yes | yes | Preserves input type and is nullable; row choice follows Spark's aggregate input order. |
| `last(...)` | `last` | `last(order.category, ignore_nulls=True)` | yes | yes | Preserves input type and is nullable; row choice follows Spark's aggregate input order. |
| `max_by(...)` | `max_by` | `max_by(order.category, order.total)` | yes | yes | Returns the candidate associated with the greatest order key; tied keys retain Spark's unspecified choice. |
| `min_by(...)` | `min_by` | `min_by(order.category, order.total)` | yes | yes | Returns the candidate associated with the smallest order key; tied keys retain Spark's unspecified choice. |
| `product(...)` | `product` | `product(order.quantity)` | yes | yes | Accepts numeric input and returns nullable Double. |
| `sum_distinct(...)` | `sum_distinct` | `sum_distinct(order.total)` | yes | yes | Sums distinct numeric values using Spark's numeric widening. |
| `regr_avgx(...)` | `regr_avgx` | `regr_avgx(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double mean of the independent (`x`) values. |
| `regr_avgy(...)` | `regr_avgy` | `regr_avgy(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double mean of the dependent (`y`) values. |
| `regr_count(...)` | `regr_count` | `regr_count(order.quantity, order.price)` | yes | yes | Counts paired non-null numeric observations and returns non-null Long. |
| `regr_intercept(...)` | `regr_intercept` | `regr_intercept(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double intercept. |
| `regr_r2(...)` | `regr_r2` | `regr_r2(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double coefficient of determination. |
| `regr_slope(...)` | `regr_slope` | `regr_slope(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double slope. |
| `regr_sxx(...)` | `regr_sxx` | `regr_sxx(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double sum of squared independent-variable deviations. |
| `regr_sxy(...)` | `regr_sxy` | `regr_sxy(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double sum of paired deviations. |
| `regr_syy(...)` | `regr_syy` | `regr_syy(order.quantity, order.price)` | yes | yes | Paired numeric regression aggregate returning nullable Double sum of squared dependent-variable deviations. |

## Sketch and Bitmap State

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `hll_sketch(...)` | HLL sketch state | `customers = hll_sketch(lg_config_k=12)` | yes | yes | Declares branded Binary HLL state; precision is a literal from 4 through 21. HLL state is not interchangeable with ordinary Binary or Bitmap state. |
| `bitmap(...)` | Bitmap state | `features = bitmap()` | yes | yes | Declares branded Binary Bitmap state. Bitmap state is not interchangeable with ordinary Binary or HLL state. |
| `kll_sketch(...)` | KLL sketch state | `kll_sketch()` | no | no | Status: `target-gated`. Requires the PySpark 4.1 profile; outside the PySpark 3.5/4.0 baseline. |
| `theta_sketch(...)` | Theta sketch state | `theta_sketch()` | no | no | Status: `target-gated`. Requires the PySpark 4.1 profile; outside the PySpark 3.5/4.0 baseline. |

