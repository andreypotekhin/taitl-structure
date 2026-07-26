# API Gaps

This page tracks PySpark parity gaps, postponed design items, and deliberately unsupported API surface. It is a
developer backlog aid, not a promise that Structure will become a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

See the user-facing summary in [API.ref.md](../reference/API.ref.md).

## Status

- `implemented`: shipped with the required capability, diagnostic, documentation, and verification evidence for the
  claimed target profile.
- `planned`: accepted direction or reserved API; needs implementation, diagnostics, tests, or docs. A plan may be
  created later.
- `scheduled`: accepted v4 catalog work assigned to a delivery slice. The pre-catalog gap list continues to use
  `planned` until Sprint 17 reclassifies it.
- `deferred`: deliberately postponed because its type, cardinality, determinism, or runtime contract is not yet
  sufficiently specified. It is not an implicit promise for the current release.
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

V3's scheduled gaps are complete. V4 now treats this page as input to the checked
[PySpark Transformation Coverage catalog](../reference/PySparkTransformationCoverage.md), rather than as a list of
isolated surprises. The catalog classifies every relevant PySpark 3.5.x/4.0.x transformation API as supported,
scheduled, deferred, or unsupported and links each supported entry to capability and parity evidence.

The delivery design and first ExecPlan are [V4 Transformation API Coverage](design/V4TransformationApiCoverage.md) and
[P07132601.V4-transformation-api-coverage.plan.md](planning/P07132601.V4-transformation-api-coverage.plan.md).
[V4 Caller-Owned Streaming Migration](design/V4CallerOwnedStreamingMigration.md) and
[P07152602.V4-caller-owned-streaming-migration.plan.md](planning/P07152602.V4-caller-owned-streaming-migration.plan.md)
define the dedicated bounded-streaming transformation slice. Loading, storage, catalog/table management, actions, and
streaming lifecycle ownership are excluded from this program.

## V6 Deferral Discipline

v6 uses the coverage catalog and its API ledger to schedule small typed PySpark additions, but this page remains the
durable register of postponed and deferred work. The design is [V6 PySpark API Closure](design/V6PySparkApiClosure.md)
and the release ledger is [V6 PySpark API Ledger](specifications/V6PySparkApiLedger.md). When v6 admits, postpones, or
rejects an API, update this page, the coverage JSON/reference, and the ledger in the same change. Keep the reason, the
user-facing boundary (`step`, explicit scalar UDF, `@raw`, or caller-owned PySpark), and the owning plan together so
an omitted API never becomes an implicit promise.

The following v6 candidates remain deferred until their contracts are complete: binary/encoding values, JSON/CSV
inline-schema parsing, row generators, relation set composition, relation ordering/selection, deterministic `mode`,
sampling, and physical-plan directives. Their relation-operation design/specification is
[Typed Relation Operations](design/TypedRelationOperations.md). Scalar `@special(type="udf")` is already implemented
for ordinary PySpark; its user contract is [Explicit Scalar Python UDFs](specifications/ExplicitScalarUdfs.md). It is
opt-in, type/nullability declared, warning-governed, and excluded from Spark Connect. It is not a substitute for an
unsupported symbolic operation.

The v6 scheduled relation additions are intentionally narrow: branchable typed union, relation assertions (including
parent-reference validation), bounded parent-hierarchy closure with deterministic fallback expansion, and
first-qualified priority selection. They exist to replace Search's cohort-band, relevance, and reranking hooks.
General recursive relations, dynamic-depth traversal, arbitrary graph algorithms, user-defined hierarchy traversal,
and implicit surrogate row identifiers remain deferred until a separate contract defines them.

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
| String predicates | implemented | `contains`, `startswith`, `endswith`, `like`, `ilike`, `rlike` | Typed methods keep plain and regex matching compiler-visible. |
| Collection indexing | implemented | `getItem`, `__getitem__` | Typed Array/Map result inference with nullable lookup results. |
| Struct field helpers | implemented | `getField` | Alias-aware typed `get_field(name)` complements attributes. |
| Rich casts | implemented | `cast`, `astype`, `try_cast` | Scalar casts work across targets; nullable `try_cast` requires profile `>=4.0,<4.1`. |
| Ordering modifiers | implemented | `asc`, `desc`, null ordering | Typed descriptors work in inline and reusable windows. |
| Null/NaN predicates | implemented | `isNaN` | Function-style `isnull`, `isnotnull`, and typed `isnan` keep null and NaN semantics distinct. |
| Bitwise column methods | implemented | `bitwiseAND`, `bitwiseOR`, `bitwiseXOR`, `bitwise_not` | Typed integer/long methods preserve nullability and use explicit backend capability checks. |
| Struct mutation | implemented | `with_field(..., schema=...)`, `drop_fields(..., schema=...)` | Explicit result Schema preserves the exact nested type and aliases. |
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
| Broader string helpers | implemented | `ltrim`, `rtrim`, `substring`, `split`, `regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, `levenshtein` | Typed cross-version String transformation, search, and comparison core. |
| Date/time helpers | implemented | `date_add`, `date_sub`, `datediff`, `date_trunc`, `trunc`, calendar extraction, `to_date`, `to_timestamp` | Typed Date/Timestamp temporal helper set. |
| Numeric/math helpers | implemented | `abs`, `round`, `bround`, `ceil`, `floor`, `sqrt`, `pow`, `log`, `exp`, `signum` | Typed deterministic scalar helper set. |
| Predicate helpers | implemented | `isnull`, `isnotnull`, `isnan` | Function-style null checks and typed NaN predicate. |
| Null-control functions | implemented | `nullif`, `nvl`, `nvl2`, `ifnull`, `zeroifnull`, `nanvl` | Typed fallback, branch, null, and NaN semantics are compiler-visible. |
| Hash helpers | implemented | `hash`, `xxhash64`, `md5`, `sha1`, `sha2` | Typed scalar hashes and String digests; they are not security or cross-engine identity primitives. |
| Encoding/binary helpers | deferred | `base64`, `unbase64`, `encode`, `decode` | Structure has no public binary type; use `@raw` until a binary-type design preserves exact input/output contracts. |
| JSON/XML/CSV helpers | deferred | Spark JSON, XML, CSV functions | JSON/CSV needs an inline Schema transport and normalized options contract; XML remains outside the public type model. Use `@raw` meanwhile. |
| Variant/geospatial helpers | planned | `VARIANT`, `ST_*` functions | Outside current type model. |
| Scalar Python UDFs | implemented | `@special(type="udf")`, PySpark `udf` | Ordinary PySpark row-local batch and streaming support with the existing `warn_on_udfs` warning policy; excluded from Spark Connect. |
| Python UDTFs and UDTs | unsupported | `udtf`, UDT | Use caller-owned PySpark or hooks; row expansion needs a cardinality contract. |
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
| Unbounded or non-contract stream-stream joins | unsupported | Streaming stream-stream joins | Bounded inner joins are implemented; V4 schedules bounded outer and left-semi forms. All admitted forms need explicit input modes, watermarks, event-time bounds, and state diagnostics. |
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
| Exact percentile family | implemented | `percentile`, `percentile_approx` | `percentile(...)` uses a scalar 0–1 percentage and positive literal frequency; `approx_percentile(...)` remains the bounded-memory alternative. |
| Additional stats | planned | `skewness`, `kurtosis`, `mode` | `skewness(...)` and `kurtosis(...)` are implemented; deterministic `mode(...)` is deferred because PySpark 3.5 lacks its deterministic tie option. |
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
| Array slicing and sorting variants | implemented | `slice`, `array_sort`, `reverse` | `slice(...)`, `arr_sort(...)`, and `arr_reverse(...)` preserve typed array contracts. |
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
| Event-time tumbling and sliding aggregations | implemented | `groupBy(window(...))` | Requires a prior watermark on the direct event-time grouping key or `window(event_time, ...)`; caller uses `append` or `update`. |
| Cross-mode dedupe | implemented | `dropDuplicates` / `dropDuplicatesWithinWatermark` | `drop_duplicates(...)` uses batch `dropDuplicates` and streaming bounded dedupe after a watermark. |
| Explicit bounded dedupe | implemented | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(...)` requires `streaming=True` and a preceding watermark. |
| Session-window aggregation | implemented | `session_window` | Requires a preceding watermark on the event-time field, a static positive gap, at least one ordinary grouping key, and caller-owned `append` mode. Dynamic gaps and session merge tuning remain caller configuration. |
| Chained window aggregation | deferred | `window_time`, `window(window(...))` | Needs a multi-stage state contract; do not admit merely because the individual windows are legal. |
| Bounded stream-stream outer and semi joins | implemented | left/right/full outer and left-semi stream-stream joins | Requires declared streaming inputs, watermarks on both bound event-time fields, a compiler-visible event-time bound, and caller-owned `append` mode; unmatched outer output can be delayed until watermark progress. |
| Stream-static left semi join | implemented | left-semi stream-static join | A non-stateful `exists(...)` filter when the left/current input is streaming and the right input is static. |
| Unsupported stream-static directions | unsupported | right/full/cross/anti stream-static joins | These runtime shapes are not admitted by Spark Structured Streaming; use a supported left/inner/left-semi lookup or caller-owned redesign. |
| Global/unbounded aggregation and dedupe | unsupported | global `groupBy`, unwatermarked `dropDuplicates` | Structure will not admit unbounded state. Group by a watermarked event-time key/window or bound state outside Structure. |
| Sorting, limits, analytic windows, selected-row helpers | deferred | `orderBy`, `limit`, ranking, `Window`, top-N | Caller-owned streaming logic; analytical windows remain batch-only. |
| Multiple stateful operators | deferred | chains of streaming aggregates/dedupe/joins | Needs explicit composition and state-budget policy. |
| Pandas, RDD, and state-processor boundaries | unsupported | Pandas UDF, RDD, `mapInPandas`, state processors | Use caller-owned streaming code. These APIs introduce opaque execution or user-owned state semantics that do not fit Structure's symbolic transform contract. |
| Generators | planned | `explode`, `posexplode`, `inline` | V4's row-generator gate supplies the schema/cardinality contract; each admitted generator must separately prove its streaming classification. |
| Caller-owned lifecycle APIs | unsupported | sources, sinks, triggers, checkpoints, query start/stop, `foreachBatch` | Intentionally owned by the end user; Structure only transforms supplied DataFrames. |

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
