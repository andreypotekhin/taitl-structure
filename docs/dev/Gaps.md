# API Gaps

This page tracks PySpark parity gaps, postponed design items, and deliberately unsupported API surface. It is a
developer backlog aid, not a promise that Structure will become a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

See the user-facing summary in [APIRef.md](../APIRef.md).

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
| DSL | Sprint 11 | [P07072602.V3-dsl-and-sql-function-pyspark-parity.plan.md](planning/done/P07072602.V3-dsl-and-sql-function-pyspark-parity.plan.md) |
| Joins | Sprint 12 | [P07072603.V3-join-pyspark-parity-hardening.plan.md](planning/done/P07072603.V3-join-pyspark-parity-hardening.plan.md) |
| Aggregations | Sprint 13 | [P07072604.V3-aggregation-pyspark-parity.plan.md](planning/done/P07072604.V3-aggregation-pyspark-parity.plan.md) |
| Windows | Sprint 14 | [P07072605.V3-window-pyspark-parity.plan.md](planning/done/P07072605.V3-window-pyspark-parity.plan.md) |
| Higher-Order And Collection Helpers | Sprint 15 | [P07072606.V3-collection-helper-pyspark-parity.plan.md](planning/P07072606.V3-collection-helper-pyspark-parity.plan.md) |
| Streaming | Sprint 16 | [P07122601.Streams-example-and-caller-owned-streaming.plan.md](planning/P07122601.Streams-example-and-caller-owned-streaming.plan.md) |

## DSL

### Column API

Structure currently supports a small compiler-visible Column subset:

- typed field references through schema attributes;
- nested struct field access through typed attributes;
- equality, null-safe equality, inequality, and ordering comparisons;
- boolean composition with `&`, `|`, and `~`;
- arithmetic `+`, `-`, and `*`;
  - `is_null()` and `is_not_null()`;
- `isin(...)` membership predicates;
- inclusive `between(...)` range predicates.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| String predicates | implemented | `contains`, `like`, `ilike`, `rlike` | Typed methods keep plain and regex matching compiler-visible. |
| Collection indexing | implemented | `getItem`, `__getitem__` | Typed Array/Map result inference with nullable lookup results. |
| Struct field helpers | implemented | `getField` | Alias-aware typed `get_field(name)` complements attributes. |
| Rich casts | implemented | `cast`, `astype`, `try_cast` | Scalar casts work across targets; nullable `try_cast` requires profile `>=4.0,<4.1`. |
| Ordering modifiers | implemented | `asc`, `desc`, null ordering | Typed descriptors work in inline and reusable windows. |
| Null/NaN predicates | implemented | `isNaN` | Function-style `isnull`, `isnotnull`, and typed `isnan` keep null and NaN semantics distinct. |
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
| Broader string helpers | implemented | `substring`, `split`, `regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, `levenshtein` | Typed cross-version String transformation, search, and comparison core. |
| Date/time helpers | implemented | `date_add`, `datediff`, `date_trunc` | Typed Date/Timestamp temporal helper set. |
| Numeric/math helpers | implemented | `abs`, `round`, `ceil`, `floor` | Typed deterministic scalar helper set. |
| Predicate helpers | implemented | `isnull`, `isnotnull`, `isnan` | Function-style null checks and typed NaN predicate. |
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
| Using-key joins | implemented | `join(on="key")`, `on=["k1", "k2"]` | Symbolic `on=` remains preferred. |
| Full join diagnostics hardening | implemented | `how="full"` | Name nullable sides clearly. |
| Right join diagnostics hardening | implemented | `how="right"` | Rowset API exists; projection rules stay sharp. |
| Cross join safety | implemented | `crossJoin`, `how="cross"` | Requires `allow_cartesian=True`. |
| Join strategy directives | implemented | `broadcast`, `merge`, shuffle hints | Capability-checked PySpark hints. |
| Join reordering | future | Cost-based join planning | Do not reorder source semantics casually. |
| Forward as-of joins | implemented | As-of nearest/forward patterns | Selects the earliest qualifying right row. |
| Nearest as-of joins | future | Nearest time matching | Needs tie and tolerance rules. |
| Stream-stream joins | unsupported | Streaming stream-stream joins | Needs state and watermark policy. |
| Raw SQL join predicates | unsupported | SQL strings in `on` | Use symbolic expressions or hooks. |

## Aggregations

Current support includes ordinary grouping, rollup, cube, explicit grouping sets, post-aggregate `having(...)`, common
exact aggregates, approximate count/percentile, boolean aggregates, statistical aggregates, filtered metrics,
collection aggregates, and deterministic first/last helpers.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Explicit grouping sets | implemented | Custom grouping-set levels | Lowers as generated PySpark branch unions. |
| Having predicates | implemented | SQL/PySpark post-aggregate filters | Uses aggregate-output predicate scope. |
| Aggregate aliases | future | `GroupedData.agg` aliases | Schema constructors own output aliases. |
| Exact percentile family | future | `percentile`, `percentile_approx` | Current helper is `approx_percentile(...)`. |
| Additional stats | future | `skewness`, `kurtosis`, `mode` | Wait for analytical contracts to settle. |
| Dict/list aggregate syntax | unsupported | `GroupedData.agg({"x": "sum"})` | Use typed helpers. |

## Windows

Structure supports inline ranking/lag/lead/rolling helpers and reusable window specs with explicit row/range frames.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Null ordering in window order keys | implemented | Null-ordering sort methods | Typed order descriptors render in inline and reusable windows. |
| Multiple order keys in all helpers | implemented | `Window.orderBy(*cols)` | Inline and reusable helpers preserve ordered keys. |
| Additional aggregate windows | implemented | Framed aggregates over `Window` | Boolean, statistical, and collection helpers are admitted; distinct windows stay unsupported by Spark. |
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

The streaming slice accepts compatible streaming DataFrames as inputs for row-local, watermarked stateful, and admitted
stream-stream operations. Structure intentionally does not own streaming lifecycle.

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Generated streaming sources | unsupported | `spark.readStream` | Callers own source selection and configuration. |
| Generated streaming sinks | unsupported | `DataFrame.writeStream` | Callers own sinks and side effects. |
| Triggers, checkpoints, and output modes | unsupported | `trigger`, `checkpointLocation`, `outputMode` | Callers apply lifecycle policy; Structure may report required modes. |
| Watermarks | implemented | `withWatermark` | Compiler-visible transform operation. |
| Streaming aggregations | implemented | Structured Streaming aggregations | Admitted only with a prior compiler-visible watermark. |
| Stateful streaming dedupe | implemented | `dropDuplicatesWithinWatermark` | Admitted only with a prior compiler-visible watermark. |
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
- API reference rows in [APIRef.md](../APIRef.md).
