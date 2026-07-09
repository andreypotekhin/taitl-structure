# API Gaps

This page tracks PySpark parity gaps, postponed design items, and deliberately unsupported API surface. It is a
developer backlog aid, not a promise that Structure will become a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

See the user-facing summary in [CompatibilityTables.md](../CompatibilityTables.md).

## Status

- `planned`: accepted direction or reserved API; needs implementation, diagnostics, tests, or docs.
- `future`: plausible parity work, but no admitted design slice yet.
- `unsupported`: intentionally outside the compiler-visible DSL, or incompatible with Structure's contract.

## Parity Sources

Track gaps against the default target range in [Compatibility.md](../Compatibility.md):

- PySpark 3.5.x and 4.0.x;
- ordinary PySpark DataFrame/Column APIs;
- Spark Connect for completed compiler-visible batch features.

Consult the official Spark 4.0.1 docs when expanding this page:

- PySpark Column reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html>
- PySpark SQL functions reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/functions.html>
- PySpark DataFrame join reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html>

The latest Spark docs may be useful for discovery, but features introduced after PySpark 4.0.x should not be marked
`planned` for the current target unless the target range changes.

## Beginning of v3 Schedule

The planned gaps on this page are scheduled into the beginning of v3 as focused ExecPlans. Each plan must update this
page, public compatibility tables, generated examples, and project-management progress as implementation completes.

| Gap Section | v3 Sprint | ExecPlan |
| --- | --- | --- |
| DSL | Sprint 11 | [P07072602.V3-dsl-and-sql-function-pyspark-parity.plan.md](planning/P07072602.V3-dsl-and-sql-function-pyspark-parity.plan.md) |
| Joins | Sprint 12 | [P07072603.V3-join-pyspark-parity-hardening.plan.md](planning/P07072603.V3-join-pyspark-parity-hardening.plan.md) |
| Aggregations | Sprint 13 | [P07072604.V3-aggregation-pyspark-parity.plan.md](planning/P07072604.V3-aggregation-pyspark-parity.plan.md) |
| Windows | Sprint 14 | [P07072605.V3-window-pyspark-parity.plan.md](planning/P07072605.V3-window-pyspark-parity.plan.md) |
| Higher-Order And Collection Helpers | Sprint 15 | [P07072606.V3-collection-helper-pyspark-parity.plan.md](planning/P07072606.V3-collection-helper-pyspark-parity.plan.md) |
| Streaming | Sprint 16 | [P07072607.V3-streaming-orchestration.plan.md](planning/P07072607.V3-streaming-orchestration.plan.md) |

## DSL

### Column API

Structure currently supports a small compiler-visible Column subset:

- typed field references through schema attributes;
- nested struct field access through typed attributes;
- equality, null-safe equality, inequality, and ordering comparisons;
- boolean composition with `&`, `|`, and `~`;
- arithmetic `+`, `-`, and `*`;
- `is_null()` and `is_not_null()`;
- inclusive `between(...)` range predicates.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Membership predicates | planned | `Column.isin` | Add typed literal and expression operands. |
| String predicates | planned | `contains`, `like`, `ilike`, `rlike` | Prefer named helpers over raw regex strings. |
| Collection indexing | planned | `getItem`, `__getitem__` | Needs typed Array/Map result inference. |
| Struct field helpers | planned | `getField` | Attribute access covers typed structs today. |
| Rich casts | planned | `cast`, `astype`, `try_cast` | Current public cast helper is `to_decimal(...)`. |
| Ordering modifiers | planned | `asc`, `desc`, null ordering | Current helpers mostly use `descending=`. |
| Null/NaN predicates | future | `isNaN` | Needs type checks and separate null-vs-NaN diagnostics. |
| Bitwise column methods | future | `bitwiseAND`, `bitwiseOR`, `bitwiseXOR` | Needs bitwise helpers first. |
| Struct mutation | future | `withField`, `dropFields` | Postponed until nested projection and whole-field copying are stable. |
| Column alias/name methods | unsupported | `alias`, `name` | Schema constructors and field aliases own output names. |
| Raw `over(...)` windows | unsupported | `Column.over` | Structure uses compiler-visible window helpers instead. |
| Raw Python truthiness | unsupported | `Column.__bool__` | Use symbolic predicates. |

### SQL Functions

Structure intentionally exposes a small helper set. The first pass covers row-local string normalization, decimal
conversion, null coalescing, conditional expressions, grouped aggregates, window helpers, and selected array/map
higher-order functions.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Broader string helpers | planned | `substring`, `split`, `regexp_replace` | Keep regex behavior explicit. |
| Date/time helpers | planned | `date_add`, `datediff`, `date_trunc` | Needed for temporal transforms. |
| Numeric/math helpers | planned | `abs`, `round`, `ceil`, `floor` | Admit deterministic scalar helpers first. |
| Predicate helpers | planned | `isnull`, `isnotnull`, `isnan` | Methods cover part of this. |
| Hash helpers | future | `hash`, `xxhash64`, `sha2`, `md5` | Needs stability notes. |
| Encoding/binary helpers | future | `base64`, `unbase64`, `encode`, `decode` | Lower priority. |
| JSON/XML/CSV helpers | future | Spark JSON, XML, CSV functions | Needs schema contracts. |
| Variant/geospatial helpers | future | `VARIANT`, `ST_*` functions | Outside current type model. |
| UDF/UDTF symbolic helpers | unsupported | `udf`, `udtf`, UDT | Use hooks for opaque PySpark. |
| Raw SQL string expressions | unsupported | `expr`, `call_function` | Keep compiler-visible expressions structured. |

## Joins

Current rowset join support is described by [FullPySparkJoinSupport.md](../reference/FullPySparkJoinSupport.md). Older
v1 lookup semantics remain in [JoinSemantics.md](../reference/JoinSemantics.md), and existence/temporal/as-of joins
are in [AnalyticalJoinCoverage.md](../reference/AnalyticalJoinCoverage.md).

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Using-key joins | planned | `join(on="key")`, `on=["k1", "k2"]` | Symbolic `on=` remains preferred. |
| Full join diagnostics hardening | planned | `how="full"` | Name nullable sides clearly. |
| Right join diagnostics hardening | planned | `how="right"` | Rowset API exists; projection rules need to stay sharp. |
| Cross join safety | planned | `crossJoin`, `how="cross"` | Requires `allow_cartesian=True`. |
| Join strategy directives | planned | `broadcast`, `merge`, shuffle hints | Broadcast exists; others are gated. |
| Join reordering | future | Cost-based join planning | Do not reorder source semantics casually. |
| Forward as-of joins | planned | As-of nearest/forward patterns | Backward as-of is the initial supported direction. |
| Nearest as-of joins | future | Nearest time matching | Needs tie and tolerance rules. |
| Stream-stream joins | unsupported | Streaming stream-stream joins | Needs state and watermark policy. |
| Raw SQL join predicates | unsupported | SQL strings in `on` | Use symbolic expressions or hooks. |

## Aggregations

Current support includes ordinary grouping, rollup, cube, common exact aggregates, approximate count/percentile,
boolean aggregates, statistical aggregates, filtered metrics, collection aggregates, and deterministic first/last
helpers.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Explicit grouping sets | planned | `groupingSets`, SQL `GROUPING SETS` | Helper is reserved; backend is deferred. |
| Having predicates | planned | SQL/PySpark post-aggregate filters | Needs aggregate-output predicate scope. |
| Aggregate aliases | future | `GroupedData.agg` aliases | Schema constructors own output aliases. |
| Exact percentile family | future | `percentile`, `percentile_approx` | Current helper is `approx_percentile(...)`. |
| Additional stats | future | `skewness`, `kurtosis`, `mode` | Wait for analytical contracts to settle. |
| Dict/list aggregate syntax | unsupported | `GroupedData.agg({"x": "sum"})` | Use typed helpers. |

## Windows

Structure supports inline ranking/lag/lead/rolling helpers and reusable window specs with explicit row/range frames.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Null ordering in window order keys | planned | Null-ordering sort methods | Needs ordering wrappers. |
| Multiple order keys in all helpers | planned | `Window.orderBy(*cols)` | Normalize order lists consistently. |
| Additional aggregate windows | planned | Aggregates over `Window` | Mirror admitted aggregate helpers. |
| Raw `WindowSpec` escape hatch | unsupported | Direct PySpark `WindowSpec` | Use hooks for raw PySpark. |

## Higher-Order And Collection Helpers

Current support includes `arr_transform`, `arr_filter`, `arr_exists`, `arr_forall`, `arr_zip_with`, `arr_aggregate`,
`arr_sort_by`, `arr_flatten`, `arr_distinct`, `arr_position`, `map_transform_values`, `map_filter`,
`map_transform_keys`, `map_zip_with`, `map_keys`, `map_values`, `map_entries`, and `map_from_entries`.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Collection size and membership | planned | `size`, `array_contains`, `map_contains_key` | Add common boolean/count helpers. |
| Array construction and set operations | planned | `array`, `array_repeat`, `array_union`, `array_except` | Needs type unification. |
| Array slicing and sorting variants | future | `slice`, `sort_array`, `reverse` | Needs null ordering docs. |
| Element lookup and map concatenation | planned | `element_at`, `try_element_at`, `map_concat` | Needs missing-key nullability. |
| Explode/generator helpers | future | `explode`, `posexplode`, `inline` | Needs row-expansion design. |
| Python control flow in callbacks | unsupported | Arbitrary Python lambdas | Return symbolic expressions only. |

## Streaming

The first streaming slice accepts compatible streaming DataFrames as inputs for row-local and limited stream-static
operations. Structure does not yet own streaming lifecycle or stateful streaming contracts.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Generated streaming sources | planned | `spark.readStream` | Sprint 16 lifecycle declaration work. |
| Generated streaming sinks | planned | `DataFrame.writeStream` | Needs checkpoint and query policy. |
| Triggers, checkpoints, and output modes | planned | `trigger`, `checkpointLocation`, `outputMode` | Required lifecycle policy for generated streaming jobs. |
| Watermarks | planned | `withWatermark` | Required before most stateful streaming features. |
| Streaming aggregations | planned | Structured Streaming aggregations | Admit only bounded, watermarked state semantics. |
| Stateful streaming dedupe | planned | `dropDuplicatesWithinWatermark` | Depends on watermark and state policy. |
| `foreachBatch` and custom sinks | unsupported | `foreachBatch`, `foreach` | Keep side effects outside the DSL. |

## Admission Checklist

Before moving a gap to implemented, add or update:

- public reference docs and examples;
- backend capability support or an explicit unsupported diagnostic;
- symbolic execution and IR tests;
- generated PySpark rendering tests;
- online execution tests when the feature runs online;
- Spark Connect evidence when the feature is claimed for that variant;
- streaming compatibility classification when the feature can receive streaming inputs;
- compatibility table rows in [CompatibilityTables.md](../CompatibilityTables.md).
