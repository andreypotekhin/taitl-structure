# API Catalog

This catalog is the source for API compatibility decisions. Use `@raw` or caller-owned PySpark if it says the symbolic contract is deferred or unsupported.

## Column API

Structure supports typed field references, nested struct field access, equality and ordering comparisons, boolean
composition, arithmetic `+`, `-`, and `*`, null predicates, `isin(...)`, and inclusive `between(...)`.

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| String predicates | implemented | `contains`, `startswith`, `endswith`, `like`, `ilike`, `rlike` | Typed plain and regex matching | [Expressions API](api/Expressions.api.md) |
| Collection indexing | implemented | `getItem`, `__getitem__` | Typed Array/Map result inference with nullable lookup results | [Collections API](api/Collections.api.md) |
| Struct field helpers | implemented | `getField` | Alias-aware typed `get_field(name)` complements attributes | [Expressions API](api/Expressions.api.md) |
| Rich casts | implemented | `cast`, `astype`, `try_cast` | Scalar casts work across targets; nullable `try_cast` requires profile `>=4.0,<4.1` | [Compatibility.md](Compatibility.md) |
| Ordering modifiers | implemented | `asc`, `desc`, null ordering | Typed descriptors work in inline and reusable windows | [Windows API](api/Windows.api.md) |
| Null/NaN predicates | implemented | `isNaN` | Function-style `isnull`, `isnotnull`, and typed `isnan` keep null and NaN semantics distinct | [Expressions API](api/Expressions.api.md) |
| Bitwise column methods | implemented | `bitwiseAND`, `bitwiseOR`, `bitwiseXOR`, `bitwise_not` | Typed integer/long methods preserve nullability and use capability checks | [Expressions API](api/Expressions.api.md) |
| Struct mutation | implemented | `withField`, `dropFields` | Explicit result Schema preserves exact nested type and aliases | [Expressions API](api/Expressions.api.md) |
| Lambda-bound struct field access | implemented | Struct field access inside higher-order callbacks | Typed struct attributes remain available inside symbolic array/map callbacks | [APIExtensions.md](APIExtensions.md) |
| Column alias/name methods | unsupported | `alias`, `name` | Schema constructors and field aliases own output names | Use schema declarations |
| Raw `over(...)` windows | unsupported | `Column.over` | Structure uses compiler-visible window helpers instead | [Windows API](api/Windows.api.md) |
| Raw Python truthiness | unsupported | `Column.__bool__` | Use symbolic predicates | [Expressions API](api/Expressions.api.md) |

## SQL Functions

Structure exposes row-local string normalization, decimal conversion, null coalescing, conditionals, grouped aggregates,
window helpers, and selected array/map higher-order functions.

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| Broader string helpers | implemented | `ltrim`, `rtrim`, `substring`, `split`, `regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, `levenshtein` | Typed cross-version string transformation, search, and comparison core | [Expressions API](api/Expressions.api.md) |
| Date/time helpers | implemented | `date_add`, `date_sub`, `datediff`, `date_trunc`, `trunc`, calendar extraction, `to_date`, `to_timestamp` | Typed Date/Timestamp temporal helper set | [Expressions API](api/Expressions.api.md) |
| Numeric/math helpers | implemented | `abs`, `round`, `bround`, `ceil`, `floor`, `sqrt`, `pow`, `log`, `exp`, `signum` | Typed deterministic scalar helper set | [Expressions API](api/Expressions.api.md) |
| Predicate helpers | implemented | `isnull`, `isnotnull`, `isnan` | Function-style null checks and typed NaN predicate | [Expressions API](api/Expressions.api.md) |
| Null-control functions | implemented | `nullif`, `nvl`, `nvl2`, `ifnull`, `zeroifnull`, `nanvl` | Typed fallback, branch, null, and NaN semantics | [Expressions API](api/Expressions.api.md) |
| Hash helpers | implemented | `hash`, `xxhash64`, `md5`, `sha1`, `sha2` | Typed scalar hashes and string digests; not security or cross-engine identity primitives | [Expressions API](api/Expressions.api.md) |
| Encoding/binary helpers | deferred | `base64`, `unbase64`, `encode`, `decode` | No public Binary type yet | Use `@raw` |
| JSON/XML/CSV helpers | deferred | Spark JSON, XML, CSV functions | JSON/CSV needs inline Schema transport and normalized options; XML remains outside the public type model | Use `@raw` |
| Variant/geospatial helpers | planned | `VARIANT`, `ST_*` functions | Outside current type model | Future type-model work |
| Scalar Python UDFs | implemented | PySpark `udf`; `@special(type="udf")` | Ordinary PySpark row-local batch and streaming support with warning policy | Spark Connect unsupported |
| Python UDTFs and UDTs | unsupported | `udtf`, UDT | Row expansion and custom type semantics are caller-owned | Use caller-owned PySpark or hooks |
| Raw SQL string expressions | unsupported | `expr`, `call_function` | Compiler-visible expressions stay structured | Use typed helpers or hooks |

## Joins

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| Using-key joins | implemented | `join(on="key")`, `on=["k1", "k2"]` | Symbolic `on=` remains preferred | [Joins API](api/Joins.api.md) |
| Full join diagnostics hardening | implemented | `how="full"` | Nullable sides are named clearly | [Joins API](api/Joins.api.md) |
| Right join diagnostics hardening | implemented | `how="right"` | Rowset API exists; projection rules stay explicit | [Joins API](api/Joins.api.md) |
| Cross join safety | implemented | `crossJoin`, `how="cross"` | Requires `allow_cartesian=True` | [Joins API](api/Joins.api.md) |
| Join strategy directives | implemented | `broadcast`, `merge`, shuffle hints | Capability-checked PySpark hints | [Joins API](api/Joins.api.md) |
| Join reordering | planned | Cost-based join planning | Do not reorder source semantics casually | Future planning |
| Forward as-of joins | implemented | As-of nearest/forward patterns | Selects the earliest qualifying right row | [Joins API](api/Joins.api.md) |
| Nearest as-of joins | planned | Nearest time matching | Needs tie and tolerance rules | Future planning |
| Unbounded or non-contract stream-stream joins | unsupported | Streaming stream-stream joins | Only admitted bounded forms are allowed; all need input modes, watermarks, event-time bounds, and state diagnostics | [Streaming API](api/Streaming.api.md) |
| Raw SQL join predicates | unsupported | SQL strings in `on` | Use symbolic expressions or hooks | [Joins API](api/Joins.api.md) |

## Aggregations

Structure supports ordinary grouping, rollup, cube, explicit grouping sets, `having(...)`, common exact aggregates,
approximate count/percentile, boolean aggregates, statistical aggregates, filtered metrics, collection aggregates, and
deterministic first/last helpers.

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| Explicit grouping sets | implemented | Custom grouping-set levels | Lowers as generated PySpark branch unions | [Aggregations API](api/Aggregations.api.md) |
| Having predicates | implemented | SQL/PySpark post-aggregate filters | Uses aggregate-output predicate scope | [Aggregations API](api/Aggregations.api.md) |
| Implicit global aggregation | implemented | Global aggregate without grouping keys | Aggregate-only steps retain global semantics and enforce empty-input nullability | [Aggregations API](api/Aggregations.api.md) |
| Ordered `collect_list` | implemented | Ordered collection aggregate | Explicit ascending/descending aggregate keys preserve deterministic collection order | [Aggregations API](api/Aggregations.api.md) |
| Aggregate aliases | planned | `GroupedData.agg` aliases | Schema constructors own output aliases | Future planning |
| Exact percentile family | implemented | `percentile`, `percentile_approx` | `percentile(...)` uses scalar 0-1 percentage and positive literal frequency; `approx_percentile(...)` is bounded-memory | [Aggregations API](api/Aggregations.api.md) |
| Additional stats | planned | `skewness`, `kurtosis`, `mode` | `skewness(...)` and `kurtosis(...)` are implemented; deterministic `mode(...)` is deferred because PySpark 3.5 lacks its deterministic tie option | [Aggregations API](api/Aggregations.api.md) |
| Deterministic selected-row helpers | implemented | Ordered aggregate/window selection patterns | `earliest_by`, `latest_by`, `dedupe_earliest_by`, and `dedupe_latest_by` encode deterministic row-selection policy | [Aggregations API](api/Aggregations.api.md) |
| Dict/list aggregate syntax | unsupported | `GroupedData.agg({"x": "sum"})` | Use typed helpers | [Aggregations API](api/Aggregations.api.md) |

## Windows

Structure supports inline ranking/lag/lead/rolling helpers and reusable window specs with explicit row/range frames.

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| Null ordering in window order keys | implemented | Null-ordering sort methods | Typed order descriptors render in inline and reusable windows | [Windows API](api/Windows.api.md) |
| Multiple order keys in all helpers | implemented | `Window.orderBy(*cols)` | Inline and reusable helpers preserve ordered keys | [Windows API](api/Windows.api.md) |
| Additional aggregate windows | implemented | Framed aggregates over `Window` | Boolean, statistical, and collection helpers are admitted; distinct windows stay unsupported by Spark | [Windows API](api/Windows.api.md) |
| Partitioned `window_max` | implemented | Window aggregate over partition/order/frame | Explicit typed window validation keeps partitioned maximum compiler-visible | [Windows API](api/Windows.api.md) |
| Raw `WindowSpec` escape hatch | unsupported | Direct PySpark `WindowSpec` | Use hooks for raw PySpark | [Windows API](api/Windows.api.md) |

## Higher-Order And Collection Helpers

Structure supports `arr_transform`, `arr_filter`, `arr_exists`, `arr_forall`, `arr_zip_with`, `arr_aggregate`,
`arr_sort_by`, `arr_flatten`, `arr_distinct`, `arr_position`, `map_transform_values`, `map_filter`,
`map_transform_keys`, `map_zip_with`, `map_keys`, `map_values`, `map_entries`, and `map_from_entries`.

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| Collection size and membership | implemented | `size`, `array_contains`, `map_contains_key` | Typed count and membership helpers preserve Spark null semantics | [Collections API](api/Collections.api.md) |
| Array construction and set operations | implemented | `array`, `array_repeat`, `array_union`, `array_except` | Compatible numerics widen; other element types must agree | [Collections API](api/Collections.api.md) |
| Array slicing and sorting variants | implemented | `slice`, `array_sort`, `reverse` | `slice(...)`, `arr_sort(...)`, and `arr_reverse(...)` preserve typed array contracts | [Collections API](api/Collections.api.md) |
| Element lookup and map concatenation | implemented | `element_at`, `try_element_at`, `map_concat` | Lookup results are nullable; safe lookup avoids out-of-range errors; map concat rejects duplicate-key policy overrides | [Collections API](api/Collections.api.md) |
| `posexplode` over array of structs | implemented | `posexplode` | `posexplode_struct(...)` expands `array<struct>` with a declared generated scope | [Collections API](api/Collections.api.md) |
| Other generator forms | deferred | `explode`, outer generators, `inline` | Needs row-expansion design; each form must define schema, cardinality, null, and streaming contracts | Use `@raw` |
| Python control flow in callbacks | unsupported | Arbitrary Python lambdas | Return symbolic expressions only | [Collections API](api/Collections.api.md) |

## Relation Operations

Relation operations change the active rowset's identity, cardinality, ordering, or available relation aliases. They are
Structure additions over public DataFrame transformation patterns, not raw DataFrame escape hatches.

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| Exact-schema set operations | implemented | `union`, `unionByName`, `intersect`, `intersectAll`, `subtract`, `exceptAll` | Exact-schema relation set composition uses Spark duplicate/distinct semantics and makes no ordering claim | Missing-column composition deferred |
| Branchable typed union | implemented | Union of compatible DataFrames | Independently materialized exact-schema lanes can converge through `union_all(...)` | Relevance-context expansion remains `@raw` |
| `relation_alias` self joins | implemented | DataFrame aliases for self joins | Named typed occurrence of the active rowset or an unjoined relation | [Joins API](api/Joins.api.md) |
| Relation order/limit/offset | implemented | `orderBy`, `limit`, `offset` | Typed order descriptors and literal bounds; bounds require ordered current relation state | `sample` deferred |
| `exactly_one` validation | implemented | Relation cardinality assertion | Declared assertion fails zero/multiple matches with `REL-E0701` | Search query construction remains `@raw` |
| `require_unique` / `require_all` / `require_reference` | implemented | Spark-plan assertions | Key, predicate, and nullable parent-reference checks fail through `REL-E0702`/`REL-E0703`/`REL-E0704` | [APIExtensions.md](APIExtensions.md) |
| First-qualified priority selection | implemented | Priority row selection pattern | `select_first_qualified(...)` selects at most one eligible row per key and reports `REL-E0705` for configured missing/tie failures | Reranking remains `@raw` |
| Bounded parent hierarchy and fallbacks | scheduled | Hierarchy expansion patterns | Literal depth plus missing-parent/cycle policies and ordered fallback selection | Cohort traversal remains `@raw` |
| Sampling | deferred | `sample` | Seed, replacement, and reproducibility contract is incomplete | Use `@raw` |
| Bounded ordered `scan(...)` | scheduled | Ordered recurrence pattern | Separate typed recurrence plan | Sprint 26 |
| Matrix inversion | intentional raw | Driver-side numerical algorithm | Not a symbolic distributed DataFrame transformation | School example hook |

## Streaming

The streaming slice accepts compatible streaming DataFrames as inputs for row-local, watermarked stateful, and admitted
stream-stream operations. Structure intentionally does not own streaming lifecycle.

| API / Capability | Status | PySpark parity | Structure contract | Reference / boundary |
| --- | --- | --- | --- | --- |
| Generated streaming sources | unsupported | `spark.readStream` | Callers own source selection and configuration | [Streaming API](api/Streaming.api.md) |
| Generated streaming sinks | unsupported | `DataFrame.writeStream` | Callers own sinks and side effects | [Streaming API](api/Streaming.api.md) |
| Triggers, checkpoints, and output modes | unsupported | `trigger`, `checkpointLocation`, `outputMode` | Callers apply lifecycle policy; Structure may report required modes | [Streaming API](api/Streaming.api.md) |
| Watermarks | implemented | `withWatermark` | Compiler-visible transform operation | [Streaming API](api/Streaming.api.md) |
| Event-time tumbling and sliding aggregations | implemented | `groupBy(window(...))` | Requires a prior watermark on the direct event-time grouping key or `window(event_time, ...)`; caller uses `append` or `update` | [Streaming API](api/Streaming.api.md) |
| Cross-mode dedupe | implemented | `dropDuplicates`, `dropDuplicatesWithinWatermark` | `drop_duplicates(...)` uses batch `dropDuplicates` and streaming bounded dedupe after a watermark | [Streaming API](api/Streaming.api.md) |
| Explicit bounded dedupe | implemented | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(...)` requires `streaming=True` and a preceding watermark | [Streaming API](api/Streaming.api.md) |
| Session-window aggregation | implemented | `session_window` | Requires a preceding watermark on the event-time field, a static positive gap, at least one ordinary grouping key, and caller-owned `append` mode | [Streaming API](api/Streaming.api.md) |
| Chained window aggregation | deferred | `window_time`, `window(window(...))` | Needs a multi-stage state contract | Use caller-owned PySpark |
| Bounded stream-stream outer and semi joins | implemented | Left/right/full outer and left-semi stream-stream joins | Requires declared streaming inputs, watermarks on both bound event-time fields, a compiler-visible event-time bound, and caller-owned `append` mode | [Streaming API](api/Streaming.api.md) |
| Stream-static left semi join | implemented | Left-semi stream-static join | Non-stateful `exists(...)` filter when the active input is streaming and the right input is static | [Streaming API](api/Streaming.api.md) |
| Unsupported stream-static directions | unsupported | Right/full/cross/anti stream-static joins | These runtime shapes are not admitted by Spark Structured Streaming | Use supported left/inner/left-semi lookup or caller-owned redesign |
| Global/unbounded aggregation and dedupe | unsupported | Global `groupBy`, unwatermarked `dropDuplicates` | Structure will not admit unbounded state | Group by watermarked event time/window or bound state outside Structure |
| Sorting, limits, analytic windows, selected-row helpers | deferred | `orderBy`, `limit`, ranking, `Window`, top-N | Caller-owned streaming logic; analytical windows remain batch-only | Use caller-owned PySpark |
| Multiple stateful operators | deferred | Chains of streaming aggregates/dedupe/joins | Needs explicit composition and state-budget policy | Use caller-owned PySpark |
| Pandas, RDD, and state-processor boundaries | unsupported | Pandas UDF, RDD, `mapInPandas`, state processors | Opaque execution and user-owned state semantics do not fit Structure's symbolic transform contract | Use caller-owned streaming code |
| Generators | planned | `explode`, `posexplode`, `inline` | Row-generator gate supplies schema/cardinality contract; each admitted generator must prove streaming classification | `posexplode_struct(...)` is batch-only today |
| Caller-owned lifecycle APIs | unsupported | Sources, sinks, triggers, checkpoints, query start/stop, `foreachBatch` | Structure only transforms supplied DataFrames | Caller-owned lifecycle |

## API Coverage

This section is Structure's checked catalog for its PySpark `>=3.5,<4.1` transformation baseline. It covers typed
transformations over caller-supplied DataFrames, not readers, writers, sessions, catalog/table management, actions, or
streaming lifecycle. The machine-checked inventory and entries remain in
[`pyspark-transformation-inventory.json`](../src/structure/plugin/pyspark/resources/pyspark-transformation-inventory.json)
and
[`pyspark-transformation-coverage.json`](../src/structure/plugin/pyspark/resources/pyspark-transformation-coverage.json).

| PySpark family | Status | Structure spelling or alternative | Contract / notes |
| --- | --- | --- | --- |
| Column comparisons, boolean, arithmetic | supported | Symbolic operators, `between`, `isin`, null predicates | Typed expressions preserve rows. |
| Column bitwise operations | supported | `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` | Integer/long only; preserves rows. |
| Column string predicates | supported | `contains`, `startswith`, `endswith`, `like`, `ilike`, `rlike` | Typed String predicates preserve rows. |
| Column cast and nested access | supported | `cast`, `try_cast`, attributes, `get_field`, indexing | `try_cast` is capability checked. |
| Struct mutation | supported | `with_field`, `drop_fields` | Requires exact declared struct shape. |
| Column alias and raw `over` | unsupported | Schema fields; typed window helpers | Names and window contracts remain compiler-visible. |
| Conditional/null/string/numeric/temporal functions | supported | `when`, `coalesce`, `nullif`, typed scalar helpers | Exact type and nullability semantics are compiler-visible. |
| Hash and encoding | mixed | Typed hashes; `@raw` for binary encoding | Binary helpers await a public Binary type. |
| JSON/CSV conversion | deferred | `@raw` hook | Inline Schema transport and stable option normalization need a dedicated IR contract. |
| Array construction, lookup, transformation | supported | Typed array helpers | Exact element/nullability and callback rules are validated. |
| Map functions | supported | Typed map helpers | Callback bodies remain symbolic. |
| Generator variants | supported/deferred | `posexplode_struct(...)`; `@raw` for other variants | The admitted generator expands `array<struct>` values with a declared generated scope and ordinal. |
| Projection and filtering | supported | Schema projection and `where` | Schema owns output names and replacement. |
| Joins and hints | supported | Typed join helpers, `relation_alias` | Explicit schema/cardinality; cross needs opt-in; self joins require named aliases. |
| Set operations | supported/deferred | `union_all`, `union_by_name`, `intersect`, `intersect_all`, `subtract`, `except_all`; `@raw` for missing-column union | Exact-schema set operations are supported for inputs and independently materialized typed lanes. |
| Ordering/limit/sample | supported/deferred | `order_by`, `limit`, `offset`; `@raw` for `sample` | Ordered bounds are compiler-visible and batch-only; sampling waits for seed/replacement/reproducibility semantics. |
| Priority selection | supported | `select_first_qualified` | Declared business keys, eligibility, and priority order select one row per key; configured missing/tie failures report `REL-E0705`. |
| Distinct and deduplication | supported | `distinct`, `drop_duplicates` | Watermark form is streaming classified. |
| Grouping and standard aggregates | supported | `group_by`, `rollup`, `cube`, typed aggregates | Declared aggregate output schema. |
| Exact percentile and statistics | mixed | `percentile`, approximate and moment helpers; `@raw` for `mode` | Deterministic mode tie-breaking is unavailable in the 3.5 baseline. |
| Ranking, selection, aggregate windows | supported | Typed window helpers | Raw `WindowSpec` is unsupported. |
| Watermarks | supported | `watermark` | Caller owns source, sink, trigger, output mode, and lifecycle. |
| Session window | supported | `session_window(event_time, gap)` | Static positive gap returns a typed `TimeWindow` grouping key. |
| Bounded stream-stream outer/semi joins | supported | `rowset_join(..., how=Join.LEFT|RIGHT|FULL)`, `exists(...)` | Both streams require watermarks and `event_time_between(...)`; caller uses append mode. |
| Stream-static semi filtering | supported | `exists(...)` | The streaming relation stays on the left; it has no state or output-mode requirement. |

Excluded categories stay caller-owned: readers, writers, storage, catalogs, sessions, table management, actions,
materializers, streaming lifecycle operations, Python UDTFs, Pandas APIs, RDD APIs, and arbitrary callback APIs.
Existing scalar `@special(type="udf")` remains its documented ordinary-PySpark exception.

## Reference

[API.ref.md](reference/API.ref.md).
