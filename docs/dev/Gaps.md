# API Gaps

This page tracks PySpark parity gaps, postponed design items, and deliberately unsupported API surface. It is a
developer backlog aid, not a promise that Structure will become a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

See the user-facing summary in [API.ref.md](../reference/API.ref.md).

## Status

- `planned`: accepted direction or reserved API; needs implementation, diagnostics, tests, or docs. A plan may be
  created later.
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

## V4 Coverage Program

V3's scheduled gaps are complete. V4 now treats this page as input to a checked transformation coverage catalog rather
than as a list of isolated surprises. The catalog will classify every relevant PySpark 3.5.x/4.0.x transformation API
as supported, scheduled, deferred, or unsupported and will link each supported entry to capability and parity evidence.

The delivery design and first ExecPlan are [V4 Transformation API Coverage](design/V4TransformationApiCoverage.md) and
[P07132601.V4-transformation-api-coverage.plan.md](planning/P07132601.V4-transformation-api-coverage.plan.md).
Loading, storage, catalog/table management, actions, and streaming lifecycle ownership are excluded from this program.

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
| Bitwise column methods | planned | `bitwiseAND`, `bitwiseOR`, `bitwiseXOR` | Needs bitwise helpers first. |
| Struct mutation | planned | `withField`, `dropFields` | Postponed until nested projection and whole-field copying are stable. |
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
| Hash helpers | planned | `hash`, `xxhash64`, `sha2`, `md5` | Needs stability notes. |
| Encoding/binary helpers | planned | `base64`, `unbase64`, `encode`, `decode` | Lower priority. |
| JSON/XML/CSV helpers | planned | Spark JSON, XML, CSV functions | Needs schema contracts. |
| Variant/geospatial helpers | planned | `VARIANT`, `ST_*` functions | Outside current type model. |
| UDF/UDTF symbolic helpers | unsupported | `udf`, `udtf`, UDT | Use hooks for opaque PySpark. |
| Raw SQL string expressions | unsupported | `expr`, `call_function` | Keep compiler-visible expressions structured. |

## Joins

Current rowset join support is described by [FullPySparkJoinSupport.back.md](../background/FullPySparkJoinSupport.back.md).
Older v1 lookup semantics remain in [JoinSemantics.back.md](../background/JoinSemantics.back.md), and
existence/temporal/as-of joins are in [AnalyticalJoinCoverage.back.md](../background/AnalyticalJoinCoverage.back.md).

Gaps:

| Gap | Status | Target PySpark Parity | Notes |
| --- | --- | --- | --- |
| Using-key joins | implemented | `join(on="key")`, `on=["k1", "k2"]` | Symbolic `on=` remains preferred. |
| Full join diagnostics hardening | implemented | `how="full"` | Name nullable sides clearly. |
| Right join diagnostics hardening | implemented | `how="right"` | Rowset API exists; projection rules stay sharp. |
| Cross join safety | implemented | `crossJoin`, `how="cross"` | Requires `allow_cartesian=True`. |
| Join strategy directives | implemented | `broadcast`, `merge`, shuffle hints | Capability-checked PySpark hints. |
| Join reordering | planned | Cost-based join planning | Do not reorder source semantics casually. |
| Forward as-of joins | implemented | As-of nearest/forward patterns | Selects the earliest qualifying right row. |
| Nearest as-of joins | planned | Nearest time matching | Needs tie and tolerance rules. |
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
| Aggregate aliases | planned | `GroupedData.agg` aliases | Schema constructors own output aliases. |
| Exact percentile family | planned | `percentile`, `percentile_approx` | Current helper is `approx_percentile(...)`. |
| Additional stats | planned | `skewness`, `kurtosis`, `mode` | Wait for analytical contracts to settle. |
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
| Collection size and membership | implemented | `size`, `array_contains`, `map_contains_key` | Typed count and membership helpers preserve Spark null semantics. |
| Array construction and set operations | implemented | `array`, `array_repeat`, `array_union`, `array_except` | Compatible numerics widen; other element types must agree. |
| Array slicing and sorting variants | planned | `slice`, `sort_array`, `reverse` | Needs null ordering docs. |
| Element lookup and map concatenation | implemented | `element_at`, `try_element_at`, `map_concat` | Lookup results are nullable; safe lookup avoids out-of-range errors; map concat rejects duplicate-key policy overrides. |
| Explode/generator helpers | planned | `explode`, `posexplode`, `inline` | Needs row-expansion design. |
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
- execution tests when the feature runs online;
- Spark Connect evidence when the feature is claimed for that variant;
- streaming compatibility classification when the feature can receive streaming inputs;
- API reference rows in [API.ref.md](../reference/API.ref.md).
