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
| `sum_distinct(...)` | `sum_distinct` | `sum_distinct(order.total)` |
| `min(...)` | `min` | `min(order.total)` |
| `max(...)` | `max` | `max(order.total)` |
| `avg(...)` | `avg` | `avg(order.total)` |
| `count_if(...)` | `count_if` | `count_if(order.is_paid)` |
| `any_value(...)` | `any_value` | `any_value(order.category, ignore_nulls=True)` |
| `array_agg(...)` | `array_agg` | `array_agg(order.customer_id)` |
| `first(...)`, `last(...)` | `first`, `last` | `first(order.category, ignore_nulls=True)` |
| `max_by(...)`, `min_by(...)` | `max_by`, `min_by` | `max_by(order.category, order.total)` |
| `product(...)` | `product` | `product(order.quantity)` |
| `bit_and(...)`, `bit_or(...)`, `bit_xor(...)` | `bit_and`, `bit_or`, `bit_xor` | `bit_and(order.flags)` |
| `regr_avgx(...)`, `regr_avgy(...)` | `regr_avgx`, `regr_avgy` | `regr_slope(order.quantity, order.price)` |
| `regr_count(...)` | `regr_count` | `regr_count(order.quantity, order.price)` |
| `regr_intercept(...)`, `regr_r2(...)`, `regr_slope(...)` | `regr_intercept`, `regr_r2`, `regr_slope` | `regr_r2(order.quantity, order.price)` |
| `regr_sxx(...)`, `regr_sxy(...)`, `regr_syy(...)` | `regr_sxx`, `regr_sxy`, `regr_syy` | `regr_sxy(order.quantity, order.price)` |

**Details And Differences**

- Named keys in `group_by(...)` determine output names.
- Every metric helper in this section accepts a symbolic metric-local `where=` filter, for example
  `sum(order.total, where=order.is_paid)`.
- `where=` must be Boolean. A filtered min/max/avg/sum/first/last can be null when no row qualifies.
- `sum(...)` widens Integer values to Long, Float values to Double, and Decimal precision by ten digits (capped at 38),
  matching Spark's aggregate result type.
- `avg(...)` returns Double for non-Decimal inputs; Decimal averages grow precision and scale by four digits, each
  capped at 38.
- `any_value(...)` returns the candidate type and accepts `ignore_nulls=`; because its selected row is not ordered,
  callers must not treat it as deterministic. `array_agg(...)` returns a non-null array and does not promise order.
- `first(...)` and `last(...)` return nullable candidate values and accept `ignore_nulls=`. They preserve Spark's
  aggregate input-order semantics; use `first_value(...)` or `last_value(...)` with `order_by=` when selection order
  must be explicit.
- `max_by(...)` and `min_by(...)` return the value associated with the extremal order expression. `product(...)`
  returns nullable Double. `bit_and(...)`, `bit_or(...)`, and `bit_xor(...)` require integral inputs and return
  nullable Long values.
- Regression helpers require paired numeric `y` and `x` expressions. `regr_count(...)` returns a non-null Long;
  the other regression helpers return nullable Double values when no valid paired observations exist.
- `sum_distinct(...)` accepts numeric values, removes duplicate non-null values before aggregation, and uses the same
  Integer-to-Long, Float-to-Double, and Decimal widening rules as `sum(...)`.

## Subtotals And Aggregate Metadata

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `rollup(...)` | `rollup` | `rollup(order.region, order.day)` |
| `cube(...)` | `cube` | `cube(order.region, order.channel)` |
| `grouping_sets(...)` | Explicit grouping sets | `grouping_sets((order.region,), ())` |
| `grouping_id()` | `grouping_id` | `grouping_id()` |
| `grouping(key)` | `grouping(col)` | Integer subtotal flag: 1 when the key is omitted, otherwise 0 |
| `is_grouped(...)` | Grouping metadata | `is_grouped(order.region)` |
| `having(...)` | Post-aggregate filter | `group_by(order.id).having(lambda out: out.n > 1)` |

**Details And Differences**

- `grouping_sets(...)` renders explicit grouped branches and `unionByName`; `grouping_sets(())` is a single global
  aggregate branch.
- `grouping_id()`, `grouping(key)`, and `is_grouped(...)` describe subtotal rows, whose grouping fields can be null.
  `is_grouped(key)` is true when the level omits that key; an actual null detail key has a false flag.
  `grouping(key)` preserves PySpark's integer 1/0 result, while `is_grouped(key)` returns a Boolean.

- Ordered first/last aggregates and latest/earliest row selection check winning ties lazily in both execution modes.
  Construction launches no validation jobs. Evaluated guards reject identical winning duplicates and conflicting
  records; lower-ranked ties do not fail. Null order values and rows excluded by `where=` do not compete in aggregate
  first/last. Spark can prune guarded work, so partial output does not certify the entire input.
- `having(...)` reads aggregate-output scope rather than the input row. It can be a bare statement after grouping or
  chained from `group_by(...)`, `rollup(...)`, `cube(...)`, or `grouping_sets(...)`.

## Advanced Metrics

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `bool_and(...)` | `bool_and` | `bool_and(order.is_verified)` |
| `bool_or(...)` | `bool_or` | `bool_or(order.is_priority)` |
| `every(...)` | `every` | `every(order.is_verified)` |
| `stddev(...)` | `stddev` | `stddev(order.total)` |
| `stddev_pop(...)`, `stddev_samp(...)` | `stddev_pop`, `stddev_samp` | `stddev_pop(order.total)` |
| `variance(...)` | `variance` | `variance(order.total)` |
| `var_pop(...)`, `var_samp(...)` | `var_pop`, `var_samp` | `var_pop(order.total)` |
| `median(...)` | `median` | `median(order.total)` |
| `corr(...)` | `corr` | `corr(order.price, order.quantity)` |
| `covar(...)` | `covar` | `covar(order.price, order.quantity)` |
| `covar_pop(...)` | `covar_pop` | `covar_pop(order.price, order.quantity)` |
| `mean(...)` | `mean` | `mean(order.total)` |
| `some(...)` | `some` | `some(order.is_verified)` |
| `std(...)` | `std` | `std(order.total)` |
| `approx_count_distinct(...)` | `approx_count_distinct` | `approx_count_distinct(o.customer_id, relative_sd=0.05)` |
| `approx_percentile(...)` | `approx_percentile` | `approx_percentile(order.total, 0.5, accuracy=100)` |
| `histogram_numeric(value, n_bins, *, as_, where=None)` | `histogram_numeric` | `histogram_numeric(order.total, 20, as_=LatencyBucket)` |
| `percentile(...)` | `percentile` | `percentile(order.total, 0.5)` |
| `schema_of_variant_agg(...)` | Variant schema aggregate | `schema_of_variant_agg(order.payload)` |
| `mode(...)` | `mode` | `mode(order.category, deterministic=True)` |
| `skewness(...)` | `skewness` | `skewness(order.total)` |
| `kurtosis(...)` | `kurtosis` | `kurtosis(order.total)` |
| `collect_list(...)` | `collect_list` | `collect_list(order.customer_id, order_by=order.created_at)` |
| `collect_set(...)` | `collect_set` | `collect_set(order.customer_id)` |
| `first_value(...)` | Ordered first-value aggregate | `first_value(order.id, order_by=order.created_at)` |
| `last_value(...)` | Ordered last-value aggregate | `last_value(order.id, order_by=order.created_at)` |

**Details And Differences**

- Statistical metrics, including population covariance, return nullable doubles. `collect_list(...)` can preserve an explicit `order_by=` sequence;
  without it, and for `collect_set(...)`, collection order is Spark-dependent.
- `histogram_numeric(...)` returns a nullable array of nullable `{x, y}` buckets; each bucket field is nullable, `x` keeps
  the input numeric type, and `y` is Double. Declare `as_` as a Schema with exactly nullable `x` and nullable Double
  `y`. The bin count must be a foldable Integer literal from 2 through 2,147,483,647. Decimal input is profile-gated
  to exact PySpark 4.0 because Spark 3.5.0 advertises the type but fails during histogram execution.
- `count_if(...)` accepts a Boolean expression and returns a non-null Long. `median(...)` and the population/sample
  standard-deviation and variance aliases return nullable Double values.
- `collect_list(...)` and `collect_set(...)` skip null inputs and return an empty non-null array when no values qualify.
  Ordered `collect_list(...)` accepts `asc()` or `desc()` descriptors; null-placement descriptors are rejected by its
  narrower cross-version contract.
- `first_value(...)` and `last_value(...)` aggregate forms require a scalar `order_by=` and currently use
  `"error"`; `ignore_nulls=` is supported only with `over=`.
- A filtered `first_value(...)` or `last_value(...)` masks nonqualifying order keys, so an excluded row cannot become
  the selected minimum or maximum.
- `mode(value, deterministic=False)` requires grouped keys. With `deterministic=True`, ties return the lowest
  orderable candidate across supported PySpark targets.
- `schema_of_variant_agg(...)` requires a Variant expression and returns a nullable SQL-format schema string. It is
  available only on resolved PySpark 4 profiles.
- Raw aggregate aliases are unsupported. Name aggregate outputs through the returned Schema constructor, and use schema
  field `alias=...` when the physical Spark column name must differ from the Structure field name.

## Sketches and Bitmaps

Use HLL or Bitmap state when a grouped transform must publish a reusable approximation or compact set summary. For a
final exact count, prefer `count_distinct(...)`; for an immediate approximate count, consider
`approx_count_distinct(...)`. Sketch state is useful when a later transform must consume or combine it.

### Declared state

| Structure API | Spark representation | Example |
| --- | --- | --- |
| `hll_sketch(lg_config_k=12)` | Branded Binary HLL state | `customers = hll_sketch(lg_config_k=12)` |
| `bitmap()` | Branded Binary Bitmap state | `features = bitmap()` |
| `kll_sketch()`, `theta_sketch()` | Profile-gated Binary declarations | PySpark 4.1 profile only |

`hll_sketch(...)` retains a literal precision from 4 through 21. HLL and Bitmap values cannot be cast to ordinary
`binary()` or to each other. KLL and Theta declarations require the PySpark 4.1 profile; they do not add a baseline
aggregate surface.

### Operations

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `hll_sketch_agg(value, lg_config_k=12, where=None)` | HLL construction | `hll_sketch_agg(order.customer_id)` |
| `hll_union_agg(value, allow_different_lg_config_k=False, where=None)` | HLL aggregate union | `hll_union_agg(order.sketch)` |
| `hll_union(left, right, allow_different_lg_config_k=False)` | `hll_union` | `hll_union(order.left_hll, order.right_hll)` |
| `hll_sketch_estimate(value)` | `hll_sketch_estimate` | `hll_sketch_estimate(order.customer_hll)` |
| `bitmap_construct_agg(value, where=None)` | Bitmap construction | `bitmap_construct_agg(order.position)` |
| `bitmap_or_agg(value, where=None)` | `bitmap_or_agg` | `bitmap_or_agg(order.feature_bitmap)` |
| `bitmap_count(value)` | `bitmap_count` | `bitmap_count(order.feature_bitmap)` |
| `bitmap_bit_position(value)`, `bitmap_bucket_number(value)` | Bitmap position helpers | `bitmap_bit_position(order.position)` |

**Details And Differences**

- HLL construction, HLL aggregate union, Bitmap construction, and Bitmap OR are grouped aggregates. Pairwise HLL union, estimate, Bitmap count,
  and position helpers preserve the current row.
- HLL union requires matching `lg_config_k` by default. Deliberately setting
  `allow_different_lg_config_k=True` on pairwise or aggregate union emits `SKETCH-W0802`, because Spark may reduce the
  result precision. Aggregate union carries the declared HLL precision in its result type; Spark remains responsible
  for validating serialized input sketches.
- Bitmap construction and position helpers require Integer or Long input. Sketch consumers reject raw Binary and the
  wrong opaque family before execution.
- Sketch state is compatible only with the appropriate Spark/profile implementation; it is not a portable Binary
  interchange format. In streaming, grouped operations follow ordinary watermark and output-mode rules. Callers own
  checkpoints, sinks, retention, and external exchange.

## Selection And Dedupe

| Structure API | PySpark parity | Example |
| --- | --- | --- |
| `latest_by(...)` | `row_number` selection | `latest_by(order.at, partition_by=order.customer_id)` |
| `earliest_by(...)` | `row_number` selection | `earliest_by(order.at, partition_by=order.customer_id)` |
| `dedupe_latest_by(...)` | Deterministic dedupe | `dedupe_latest_by(order.at, partition_by=order.customer_id)` |
| `dedupe_earliest_by(...)` | Deterministic dedupe | `dedupe_earliest_by(order.at, partition_by=order.customer_id)` |
| `drop_duplicates(...)` | `dropDuplicates` / `dropDuplicatesWithinWatermark` | `drop_duplicates(order.customer_id)` |
| `drop_duplicates_within_watermark` | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(...)` |
| `distinct(...)` | `distinct` | `distinct(order)` |

**Details And Differences**

- Selected-row helpers need explicit partition and scalar ordering expressions.
- `drop_duplicates(...)` accepts a same-scope field subset; `distinct(...)` can use the whole relation. For streaming
  frames it requires a preceding watermark and uses bounded `dropDuplicatesWithinWatermark`; batch frames use normal
  `dropDuplicates`. `drop_duplicates_within_watermark(...)` is the explicit streaming-only spelling.
- Operations apply in source order. See [Transforms background](../background/Transform.back.md).
