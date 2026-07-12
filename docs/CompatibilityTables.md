# Compatibility Tables

These tables summarize Structure's current compiler-visible API surface and the main PySpark parity gaps. They are
intentionally compact; detailed behavior lives in the reference pages linked from each section.

Legend:

- `check`: implemented in the public Structure API or current backend capability profile.
- `planned`: accepted direction or reserved API, but not complete enough to rely on.
- `future`: plausible later work without a committed design slice.
- `unsupported`: intentionally outside the compiler-visible DSL.

Default target: PySpark `>=3.5,<4.1`, ordinary PySpark by default, Spark Connect for completed batch features. PySpark
parity names follow the official Spark 4.0.1 Python API docs for Column, SQL functions, and DataFrame joins.

## Core DSL

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Schema classes and fields | check | `StructType`, `StructField` | Structure owns typed schema declarations. |
| Scalar types | check | Spark SQL scalar types | Common numeric, string, boolean, date, and timestamp types. |
| Array, map, struct types | check | `ArrayType`, `MapType`, `StructType` | Nested type support is compiler-visible. |
| Transform classes | check | DataFrame transform pipeline | Source-ordered step methods compile to PySpark. |
| Named inputs and outputs | check | DataFrame arguments/results | Generated and online invocation use declared names. |
| Hooks | check | Arbitrary PySpark DataFrame code | Opaque boundary through `@raw`. |
| Raw SQL expression strings | unsupported | `functions.expr`, SQL fragments | Use symbolic helpers or hooks. |
| Python UDFs in symbolic DSL | unsupported | `udf`, `udtf` | Hooks remain the honest escape hatch. |

## Column And Expression Surface

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Typed field reference | check | `Column`, `col` | Attribute access preserves schema and alias metadata. |
| Nested struct field reference | check | `Column.getField` | Typed attributes and explicit `expr.get_field(name)`. |
| Null predicates | check | `isNull`, `isNotNull` | Exposed as `is_null()` and `is_not_null()`. |
| Null-safe equality | check | `eqNullSafe` | Exposed as `null_safe_eq(...)`. |
| Boolean operators | check | `&`, `|`, `~` | Python truthiness is rejected. |
| Comparisons | check | `==`, `!=`, `<`, `<=`, `>`, `>=` | Used in filters and joins when capability permits. |
| Arithmetic | check | `+`, `-`, `*` | Narrow initial arithmetic subset. |
| Membership predicates | check | `isin` | Exposed as `expr.isin(...)`. |
| Range predicates | check | `between` | Exposed as inclusive `expr.between(lower, upper)`. |
| String predicates | check | `contains`, `like`, `ilike`, `rlike` | Typed `expr` methods; `rlike` explicitly accepts a Java regex. |
| Collection indexing | check | `getItem`, `__getitem__` | Typed `array[index]` and `map[key]` expressions return nullable lookups. |
| Struct field helper | check | `getField` | Alias-aware `expr.get_field(name)` expression. |
| Rich casts | check | `cast`, `astype`, `try_cast` | `try_cast` returns a nullable value and requires target profile `>=4.0,<4.1`. |
| Ordering modifiers | check | `asc`, `desc`, null ordering | Typed order descriptors for inline and reusable windows. |
| Struct mutation | future | `withField`, `dropFields` | Needs a nested projection design. |
| Column aliases | unsupported | `alias`, `name` | Schema fields own output names. |
| Raw `Column.over` | unsupported | `over` | Structure uses window helpers instead. |

## Scalar Function Helpers

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| `lower` | check | `functions.lower` | String normalization helper. |
| `upper` | check | `functions.upper` | String normalization helper. |
| `trim` | check | `functions.trim` | String normalization helper. |
| `to_decimal` | check | `Column.cast(DecimalType)` | Typed decimal conversion helper. |
| `coalesce` | check | `functions.coalesce` | Null fallback expression. |
| `when(...).otherwise(...)` | check | `functions.when`, `Column.otherwise` | Structured conditional expression. |
| `substring`, `split`, `regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, `levenshtein` | check | Same PySpark functions | Typed string helpers with explicit literal pattern, separator, and search contracts. |
| `date_add`, `datediff`, `date_trunc` | check | Same PySpark functions | Typed Date/Timestamp helper set. |
| Additional date/time helpers | planned | Additional SQL date/time functions | Remaining temporal parity gap. |
| `abs`, `round`, `ceil`, `floor` | check | Same PySpark functions | Typed deterministic numeric helper set. |
| Additional math helpers | planned | Additional SQL math functions | Remaining numeric parity gap. |
| `isnull`, `isnotnull`, `isnan` | check | Same PySpark functions | Function-style null/NaN predicates. |
| Hash helpers | future | `hash`, `xxhash64`, `sha2`, `md5` | Needs stability notes. |
| JSON/XML/CSV helpers | future | Spark JSON, XML, CSV functions | Needs schema and parse-error contracts. |
| Variant/geospatial helpers | future | `VARIANT`, `ST_*` | Outside current type model. |

## Joins

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Lookup left join | check | `DataFrame.join(..., how="left")` | Many-to-one or one-to-one lookup enrichment. |
| Lookup inner join | check | `DataFrame.join(..., how="inner")` | Keeps matching current rows. |
| Existence filter | check | `left_semi` join | Exposed as `exists(...)`. |
| Non-existence filter | check | `left_anti` join | Exposed as `not_exists(...)`. |
| Row-multiplying inner join | check | `inner` join | Use when one current row may produce many output rows. |
| Deterministic lookup dedupe | check | Windowed `row_number` before join | Uses `JoinDedupe` helpers. |
| Temporal validity lookup | check | Equi join plus validity predicates | Exposed as `temporal_one(...)`. |
| Backward as-of lookup | check | Windowed nearest-prior match | Exposed as `as_of_one(..., direction=AsOf.BACKWARD)`. |
| Left rowset join | check | `how="left"` | Broader rowset API. |
| Right rowset join | check | `how="right"` | Projection must handle nullable left side. |
| Full rowset join | check | `how="full"` / `full_outer` | Projection must handle nullable sides. |
| Cross join | check | `crossJoin`, `how="cross"` | Requires `allow_cartesian=True`. |
| Non-equi predicates | check | Column join expressions | Batch rowset joins. |
| Disjunctive predicates | check | Column join expressions with `OR` | Batch rowset joins. |
| Using-key joins | planned | `on="key"`, `on=["k1", "k2"]` | Symbolic `on=` remains preferred. |
| Join strategy directives | planned | Broadcast, merge, shuffle hints | Broadcast exists; others are gated. |
| Forward as-of lookup | planned | Forward as-of matching | Needs deterministic tie/tolerance rules. |
| Nearest as-of lookup | future | Nearest time matching | More subtle tie policy. |
| Stream-stream joins | unsupported | Structured Streaming joins | Deferred until streaming lifecycle support. |

## Aggregations

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| `group_by` | check | `groupBy` | Ordinary grouped aggregation. |
| `rollup` | check | `rollup` | Hierarchical totals. |
| `cube` | check | `cube` | All key combinations. |
| `count` | check | `count` | Supports metric-local `where=`. |
| `count_distinct` | check | `countDistinct`, `count_distinct` | Exact distinct count. |
| `sum`, `min`, `max`, `avg` | check | Same PySpark aggregate names | Common exact aggregates. |
| `bool_and`, `bool_or` | check | Boolean aggregate functions | Nullable boolean aggregate helpers. |
| `stddev`, `variance` | check | Statistical aggregate functions | Double result helpers. |
| `corr`, `covar` | check | Correlation/covariance aggregates | Pairwise statistical metrics. |
| `approx_count_distinct` | check | `approx_count_distinct` | Optional relative standard deviation. |
| `approx_percentile` | check | `approx_percentile` | Approximate percentile helper. |
| `collect_list`, `collect_set` | check | Collection aggregates | Ordering is Spark-dependent. |
| Ordered first/last aggregate | check | `first_value`, `last_value` patterns | Requires explicit `order_by=`. |
| `grouping_id`, `is_grouped` | check | `grouping_id`, grouping metadata | Used with rollup/cube subtotals. |
| `grouping_sets` | planned | `groupingSets`, SQL `GROUPING SETS` | Public helper reserved; backend support deferred. |
| `having` | planned | Post-aggregate filter | Needs aggregate-output predicate scope. |
| More statistical aggregates | future | `skewness`, `kurtosis`, `mode` | No committed design slice yet. |
| Dict aggregate syntax | unsupported | `GroupedData.agg({"x": "sum"})` | Use typed helper expressions. |

## Windows

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Reusable window spec | check | `Window.partitionBy(...).orderBy(...)` | Exposed as `window(...)`. |
| Row frames | check | `rowsBetween` | Explicit bounds required. |
| Range frames | check | `rangeBetween` | Type compatibility must be checked. |
| `row_number`, `rank`, `dense_rank` | check | Same PySpark names | Inline helpers. |
| `lag`, `lead` | check | Same PySpark names | Offset and default supported. |
| Rolling sum/avg/min/max | check | Aggregate over bounded row frame | Convenience helpers. |
| `percent_rank`, `cume_dist`, `ntile` | check | Same PySpark names | Reusable-window helpers. |
| `first_value`, `last_value`, `nth_value` over window | check | Same PySpark names | Optional `ignore_nulls`. |
| Window aggregate helpers | check | `sum`, `avg`, `min`, `max`, `count` | Names use `window_*`. |
| Null ordering | planned | `asc_nulls_first`, `desc_nulls_last` | Needs ordering wrappers. |
| All aggregates over windows | planned | Aggregates with `over(...)` | Extend after type rules settle. |
| Raw `WindowSpec` DSL escape hatch | unsupported | Direct PySpark `Window` objects | Use hooks for raw PySpark. |

## Higher-Order And Collection Helpers

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| `arr_transform` | check | `transform` | Symbolic array callback. |
| `arr_filter` | check | `filter` | Callback must return boolean expression. |
| `arr_exists`, `arr_forall` | check | `exists`, `forall` | Symbolic predicate callbacks. |
| `arr_zip_with` | check | `zip_with` | Symbolic two-array callback. |
| `arr_aggregate` | check | `aggregate` | Symbolic merge and optional finish callback. |
| `arr_sort_by` | check | `array_sort` with comparator/key pattern | Structure owns callback validation. |
| `arr_flatten`, `arr_distinct`, `arr_position` | check | Common array helpers | Includes position lookup. |
| `map_transform_values`, `map_transform_keys` | check | Map transform helpers | Duplicate key handling is strict. |
| `map_filter`, `map_zip_with` | check | `map_filter`, `map_zip_with` | Symbolic callbacks. |
| Map key/value/entry helpers | check | Same PySpark names | Introspection and round-trip helpers. |
| Collection size and contains | planned | `size`, `array_contains`, `map_contains_key` | Common predicate/count gap. |
| Element lookup | planned | `element_at`, `try_element_at` | Needs missing-key nullability rules. |
| Array construction/set operations | planned | `array`, `array_repeat`, `array_union`, `array_except` | Needs type unification. |
| Explode/generator helpers | future | `explode`, `posexplode`, `inline` | Row expansion needs a separate design. |
| Arbitrary Python callbacks | unsupported | Python lambda execution per row | Callbacks must remain symbolic. |

## Streaming And Backend Variants

| Structure Feature | Implementation | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Batch ordinary PySpark | check | `SparkSession`, `DataFrame`, `Column` | Default runtime target. |
| Batch Spark Connect | check | Spark Connect DataFrame/Column API | For completed compiler-visible batch features. |
| Row-local streaming projection/filter | check | Streaming DataFrame transforms | Conservative compatibility. |
| Stream-static left/inner joins | check | Supported Structured Streaming pattern | Limited to documented shapes. |
| Streaming sources | planned | `readStream` | Structure-owned lifecycle declarations in v3. |
| Streaming sinks/query lifecycle | planned | `writeStream`, `StreamingQuery` | Requires explicit checkpoint and query policy. |
| Triggers and output modes | planned | `trigger`, `outputMode` | Required generated streaming job policy. |
| Watermarks/state policy | planned | `withWatermark`, state retention | Required for admitted stateful streaming. |
| Streaming aggregations | planned | Structured Streaming aggregations | Needs bounded output-mode and state semantics. |
| Classic-only Spark Connect internals | unsupported | SparkContext, RDD, JVM, `_jdf` | Rejected by policy. |

## Sources

- [Compatibility policy](reference/CompatibilityPolicy.md)
- [Backend capabilities](reference/BackendCapabilities.md)
- [DSL reference](reference/DSL.md)
- [Join semantics](reference/JoinSemantics.md)
- [Full PySpark join support](reference/FullPySparkJoinSupport.md)
- [Advanced analytical operations](reference/AdvancedAnalyticalOperations.md)
- [Spark streaming deferred features](reference/SparkStreamingDeferredFeatures.md)
- PySpark 4.0.1 Column reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html>
- PySpark 4.0.1 SQL functions reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/functions.html>
- PySpark 4.0.1 DataFrame.join reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html>
