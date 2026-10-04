# API Compatibility

This catalog is the reference for API compatibility decisions. Use `@raw` or caller-owned PySpark if it says the symbolic contract is design-gated, streaming-ineligible, or unsupported.

For extensions on top of PySpark, see [API.md](../API.md#extensions-beyond-pyspark). For Structure-owned APIs such as schemas, transforms, and hooks, see the core API references in the [API documentation map](../API.md#api-documentation-map).

The current open contract register is maintained in [API Catalog Gates](../dev/gated/ApiCatalog.gates.md), with streaming
gates in [Streaming Gates](../dev/gated/Streaming.gates.md). Planning for remaining actionable rows is grouped in the
[API catalog and schema-evolution plan](../dev/planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md).
Companion streaming state, side-effect, and evidence plans are linked from the
[streaming project plan](../dev/project-management/V10.md). An open or gated row is not a support claim;
each entry must name its owner boundary, evidence, and caller remedy.

## Validation and selection timing

Relation assertions and ambiguity checks are lazy in online and generated execution. Constructing a result
launches no validation jobs. Caller actions evaluate guards retained by Spark; eliminated work may skip checks.
Schema-only validation remains metadata-only. See the [Relations API](../api/Relations.api.md) and
[Aggregation reference](../reference/Aggregations.ref.md) for scope and pruning limits.

## Column API

Structure supports typed field references, nested struct field access, equality and ordering comparisons, boolean
composition, arithmetic `+`, `-`, and `*`, null predicates, `isin(...)`, and inclusive `between(...)`.

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| String predicates | implemented | `contains`, `startswith`, `endswith`, `like`, `ilike`, `rlike` | Typed plain and regex matching | [Expressions API](../api/Expressions.api.md) |
| Collection indexing | implemented | `getItem`, `__getitem__` | Typed Array/Map result inference with nullable lookup results | [Collections API](../api/Collections.api.md) |
| Struct field helpers | implemented | `getField` | Alias-aware typed `get_field(name)` complements attributes | [Expressions API](../api/Expressions.api.md) |
| Rich casts | implemented | `cast`, `astype`, `try_cast` | Scalar casts work across targets; nullable `try_cast` requires profile `>=4.0,<4.1` | [Compatibility.md](../Compatibility.md) |
| Ordering modifiers | implemented | `asc`, `desc`, null ordering | Typed descriptors work in inline and reusable windows | [Windows API](../api/Windows.api.md) |
| Null/NaN predicates | implemented | `isNaN` | Function-style `isnull`, `isnotnull`, and typed `isnan` keep null and NaN semantics distinct | [Expressions API](../api/Expressions.api.md) |
| Bitwise column methods | implemented | `bitwiseAND`, `bitwiseOR`, `bitwiseXOR`, `bitwise_not` | Typed integer/long methods preserve nullability and use capability checks | [Expressions API](../api/Expressions.api.md) |
| Struct mutation | implemented | `withField`, `dropFields` | Explicit result Schema preserves exact nested type and aliases | [Expressions API](../api/Expressions.api.md) |
| Lambda-bound struct field access | implemented | Struct field access inside higher-order callbacks | Typed struct attributes remain available inside symbolic array/map callbacks | [API.md](../API.md#extensions-beyond-pyspark) |
| Column alias/name methods | unsupported | `alias`, `name` | Schema constructors and field aliases own output names | Use schema declarations |
| Raw `over(...)` windows | unsupported | `Column.over` | Structure uses compiler-visible window helpers instead | [Windows API](../api/Windows.api.md) |
| Raw Python truthiness | unsupported | `Column.__bool__` | Use symbolic predicates | [Expressions API](../api/Expressions.api.md) |

## SQL Functions

The default parity baseline is the intersection of the public PySpark 3.5.x and 4.0.x function APIs. A family is
`partial` when Structure covers useful typed functions but at least one baseline function remains open. A Structure
name may be a deliberately typed equivalent rather than the exact PySpark spelling; the compatibility columns record
that difference instead of treating it as complete parity. The detailed family tracker is in [APITracker.md](APITracker.md);
function-specific gates are indexed in [Function Gates](../dev/gated/Functions.gates.md). The implementation sequence is in the
[PySpark SQL function coverage ExecPlan](../dev/planning/P08222601.PySpark-SQL-function-coverage.plan.md).

| Function family | Status | Covered Structure surface | Remaining baseline gaps or boundary | Reference |
| --- | --- | --- | --- | --- |
| Normal, conditional, predicate, and sort functions | partial | `literal`, `when`, `coalesce`, `nullif`, `nvl`, `nvl2`, `ifnull`, `zeroifnull`, `nanvl`, `isnull`, `isnotnull`, `isnan`, `equal_null`, `assert_true`, `raise_error`, function-form `like`/`ilike`/`regexp`/`regexp_like`/`rlike` | Typed ordering descriptors are implemented. `expr` and `call_function` remain unsupported because they erase the typed expression boundary. | [Expressions API](../api/Expressions.api.md) |
| String functions | partial | `ascii`, `bit_length`, `btrim`, `char`, `char_length`, `character_length`, `contains`, `elt`, `endswith`, `find_in_set`, `format_number`, `format_string`, `lower`, `lcase`, `upper`, `ucase`, `trim`, `ltrim`, `rtrim`, `lpad`, `rpad`, `left`, `right`, `substring`, `substr`, `substring_index`, `split`, `concat_ws`, `printf`, `regexp_count`, `regexp_extract`, `regexp_extract_all`, `regexp_instr`, `regexp_replace`, `regexp_substr`, `length`, `locate`, `mask`, `octet_length`, `overlay`, `position`, `repeat`, `replace`, `initcap`, `reverse`, `soundex`, `translate`, `instr`, `levenshtein`, `split_part`, `startswith` | UTF-8 validation, collation, and `randstr` are PySpark 4.0-only and target-gated. `bit_length` is typed over String/Binary; function-form `contains`, `startswith`, and `endswith` preserve SQL null behavior. Regex patterns and capture-group indexes use the current literal-argument policy. | [Expressions API](../api/Expressions.api.md) |
| Numeric and mathematical functions | partial | `abs`, `acos`, `acosh`, `asin`, `asinh`, `atan`, `atan2`, `atanh`, `bin`, `bround`, `cbrt`, `ceil`, `ceiling`, `conv`, `cos`, `cosh`, `cot`, `csc`, `degrees`, `e`, `exp`, `expm1`, `factorial`, `floor`, `greatest`, `hex`, `hypot`, `least`, `ln`, `log`, `log1p`, `log2`, `log10`, `negate`, `negative`, `positive`, `pmod`, `pi`, `pow`, `power`, `radians`, `rint`, `round`, `sec`, `sign`, `signum`, `sin`, `sinh`, `sqrt`, `tan`, `tanh`, `unhex`, `width_bucket` | Typed numeric, base-conversion, and histogram helpers preserve row cardinality; safe arithmetic and aggregates remain design-gated. | [Expressions API](../api/Expressions.api.md) |
| Random and seeded functions | partial | `rand`, `randn` with explicit seed/reproducibility policy | PySpark 4.0 `randstr` and `uniform` are target-gated; newer random helpers need separate type, version, seed, and streaming contracts. | [Expressions API](../api/Expressions.api.md) |
| Date and timestamp functions | partial | Typed date arithmetic, String parsing/formatting, truncation/extraction, epoch conversion, query clocks, timezone conversion, timestamp construction, and qualified intervals | All 59 inventoried PySpark 3.5.x/4.0.x temporal and query-clock names have individual contracts. The official 3.5.6/4.0.0 function-index census is reconciled; semantic parity remains function- and profile-specific. Calendar Schema/Row materialization retains documented target-specific limits. | [Expressions API](../api/Expressions.api.md) |
| Bitwise and binary functions | partial | Typed `Column` bitwise methods plus SQL `bitwise_not`, `bit_count`, `bit_get`, `getbit`, `shiftleft`, `shiftright`, `shiftrightunsigned`, `base64`, `unbase64`, `encode`, `decode`, `to_binary`, `try_to_binary`, `hex`, `unhex`, and typed AES-GCM helpers | UTF-8 validation is PySpark 4.0-only and target-gated; non-GCM modes and key storage remain caller-owned. | [Expressions API](../api/Expressions.api.md) |
| Hash functions | implemented | `hash`, `xxhash64`, `crc32`, `md5`, `sha`, `sha1`, `sha2` | PySpark `sha` maps to Structure `sha1`; hashes and digests retain their non-identity and non-password-storage boundary, and CRC-32 is a checksum rather than a cryptographic digest. | [Expressions API](../api/Expressions.api.md) |
| JSON and CSV functions | partial | Schema-carrying `from_json`, `to_json`, `from_csv`, `to_csv`, typed `get_json_object`, `json_array_length`, `json_object_keys`, declared-schema `json_tuple`, and literal `schema_of_json`/`schema_of_csv` | Dynamic schema inference remains unsupported; `json_tuple` is top-level and nullable-string-only. | [Expressions API](../api/Expressions.api.md) |
| Array and higher-order functions | implemented | Typed array construction, lookup, mutation, set, concatenation, size, join, extrema, overlap, sort, shuffle, `arrays_zip`, `reduce`, and callback helpers through `arr_*`, `array_*`, `sequence`, and `slice` | Typed contracts cover array shape, nullability, nondeterminism, and callback accumulator rules; broader aliases still follow the explicit inventory disposition. | [Collections API](../api/Collections.api.md) |
| Struct and map functions | implemented | Typed `create_map`, `map_from_arrays`, `str_to_map`, `named_struct`, lookup, entries, values, and callback helpers; schema constructors own declared struct shape | Map keys are non-null; string-map delimiters and named-struct field names are validated literals. | [Collections API](../api/Collections.api.md) |
| Aggregate functions | partial | Core aggregates, boolean/statistical aggregates including `some`, `mean`, `std`, and population covariance, `count_if`, `median`, population/sample standard-deviation and variance aliases, approximate percentiles and typed `histogram_numeric`, collection aggregates, deterministic `mode`, `any_value`, `array_agg`, bitwise aggregates, `first`/`last`, `max_by`/`min_by`, `product`, `sum_distinct`, regression aggregates, and typed opaque HLL/Bitmap aggregates including `hll_union_agg` | PySpark 4.0 `string_agg`, `string_agg_distinct`, `listagg`, and `listagg_distinct` are target-gated; KLL/Theta are design-gated for V11 and require a 4.1 profile; Count-Min remains caller-owned. | [Aggregations API](../api/Aggregations.api.md#advanced-metrics) |
| Window functions | partial | Per-function typed ranking/distribution, lag/lead, value selection, and window aggregate helpers | Raw `Column.over` is unsupported; expression-valued lag/lead defaults and value-function `ignoreNulls` controls remain caller-owned. | [Windows API](../api/Windows.api.md) |
| Generators and partition transforms | partial | Typed `stack`, array/map/struct generators, and Variant TVF expansion | Generic generator spellings and writer partition transforms remain caller-owned. | [Collections API](../api/Collections.api.md) |
| Relation distribution | implemented | `coalesce(partitions=...)`, `repartition(count, *keys)`, and `repartition(*keys)` | Hash distribution preserves rows/schema and gives no ordering or stable partition identity; range repartitioning is batch-only and writer partition transforms remain caller-owned. | [Relations API](../api/Relations.api.md) |
| Variant functions | partial | Released-profile Variant parsing, extraction, validation, schema inspection, conversion, and TVF expansion | `is_valid_variant` and mutation remain target-gated until a released profile has complete evidence. | [Expressions API](../api/Expressions.api.md) |
| XML, URL, and runtime functions | partial or unsupported | Typed `url_encode(...)`, strict `url_decode(...)`, and target-gated `try_url_decode(...)`; no XML surface | XML and XPath await a declared-schema contract; runtime metadata and reflection remain caller-owned. | [Expressions API](../api/Expressions.api.md) |
| Geospatial provider APIs | target-gated | No default-baseline Geometry/Geography claim | Native `st_*` belongs to PySpark 4.1+; external providers use matching namespaces and Binary handoffs are caller-owned. | [Geospatial reference](../reference/Geospatial.ref.md) |
| Python UDF/UDTF and custom types | implemented for scalar UDFs; otherwise unsupported | Opt-in scalar `@special(type="udf")` for ordinary batch use | Pandas UDFs, UDTFs, UDTs, arbitrary callbacks, and implicit UDF conversion remain caller-owned; they cannot be counted as symbolic SQL-function coverage. | [Transforms API](../api/Transforms.api.md) |

## Joins

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Using-key joins | implemented | `join(on="key")`, `on=["k1", "k2"]` | Symbolic `on=` remains preferred | [Joins API](../api/Joins.api.md) |
| Full join diagnostics hardening | implemented | `how="full"` | Nullable sides are named clearly | [Joins API](../api/Joins.api.md) |
| Right join diagnostics hardening | implemented | `how="right"` | Rowset API exists; projection rules stay explicit | [Joins API](../api/Joins.api.md) |
| Cross join safety | implemented | `crossJoin`, `how="cross"` | Requires `allow_cartesian=True`; `param_join(...)` is the parameter shortcut with a batch-only singleton assertion | [Joins API](../api/Joins.api.md) |
| Join strategy directives | implemented | `broadcast`, `merge`, shuffle hints | Capability-checked PySpark hints | [Joins API](../api/Joins.api.md) |
| Join reordering | design-gated | Cost-based join planning | No public `join_order(...)` in the current profile; logical reordering needs dependency-safe predicate analysis and explainable selected order | [API Catalog Deferred Work](../dev/deferred/ApiCatalog.deferred.md) |
| Backward/forward as-of joins | implemented | Directional as-of matching | Selects the latest previous or earliest following qualifying right row | [Joins API](../api/Joins.api.md) |
| Nearest as-of joins | implemented | Nearest time matching | Selects the closest non-null right time and fails equidistant matches with `ties="error"` | [Joins API](../api/Joins.api.md) |
| Unbounded or non-contract stream-stream joins | unsupported | Streaming stream-stream joins | Only admitted bounded forms are allowed; all need input modes, watermarks, event-time bounds, and state diagnostics | [Streaming API](../api/Streaming.api.md) |
| Raw SQL join predicates | unsupported | SQL strings in `on` | Use symbolic expressions or hooks | [Joins API](../api/Joins.api.md) |

## Aggregations

Structure supports ordinary grouping, rollup, cube, explicit grouping sets, subtotal metadata (`grouping`, `grouping_id`,
`is_grouped`), `having(...)`, common exact aggregates,
approximate count/percentile, boolean aggregates, statistical aggregates, filtered metrics, collection aggregates, and
deterministic first/last helpers.

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Explicit grouping sets | implemented | Custom grouping-set levels | Lowers as generated PySpark branch unions | [Aggregations API](../api/Aggregations.api.md) |
| Subtotal metadata | implemented | `grouping`, `grouping_id` | Typed Integer PySpark-compatible flags/bitmasks plus Boolean `is_grouped` | [Aggregations API](../api/Aggregations.api.md) |
| Having predicates | implemented | SQL/PySpark post-aggregate filters | Uses aggregate-output predicate scope | [Aggregations API](../api/Aggregations.api.md) |
| Implicit global aggregation | implemented | Global aggregate without grouping keys | Aggregate-only steps retain global semantics and enforce empty-input nullability | [Aggregations API](../api/Aggregations.api.md) |
| Ordered `collect_list` | implemented | Ordered collection aggregate | Explicit ascending/descending aggregate keys preserve deterministic collection order | [Aggregations API](../api/Aggregations.api.md) |
| Aggregate aliases | unsupported | `GroupedData.agg` aliases | Output schema constructors and field `alias=...` own aggregate names; no second aggregate aliasing API | [Aggregations API](../api/Aggregations.api.md) |
| Exact percentile family | implemented | `percentile`, `percentile_approx` | `percentile(...)` uses scalar 0-1 percentage and positive literal frequency; `approx_percentile(...)` is bounded-memory | [Aggregations API](../api/Aggregations.api.md) |
| Additional stats | implemented | `skewness`, `kurtosis`, `mode`, `any_value`, `array_agg`, bitwise aggregates, `first`/`last`, `max_by`/`min_by`, `product`, `sum_distinct`, and regression aggregates | `mode(value, deterministic=False)` uses PySpark 4 native deterministic mode and an equivalent typed PySpark 3.5 lowering when `deterministic=True`; order-sensitive helpers retain Spark's input-order/nondeterminism semantics. Regression helpers require paired numeric inputs and expose Spark's nullable Double or non-null Long result contracts. `sum_distinct` uses numeric Spark widening and distinct-value aggregation. | [Aggregations API](../api/Aggregations.api.md) |
| Deterministic selected-row helpers | implemented | Ordered aggregate/window selection patterns | `earliest_by`, `latest_by`, `dedupe_earliest_by`, and `dedupe_latest_by` encode deterministic row-selection policy | [Aggregations API](../api/Aggregations.api.md) |
| Dict/list aggregate syntax | unsupported | `GroupedData.agg({"x": "sum"})` | Use typed helpers | [Aggregations API](../api/Aggregations.api.md) |

## Windows

Structure supports inline ranking/lag/lead/rolling helpers and reusable window specs with explicit row/range frames.

| API / Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Null ordering in window order keys | implemented | Null-ordering sort methods | Typed order descriptors render in inline and reusable windows | [Windows API](../api/Windows.api.md) |
| Multiple order keys in all helpers | implemented | `Window.orderBy(*cols)` | Inline and reusable helpers preserve ordered keys | [Windows API](../api/Windows.api.md) |
| Additional aggregate windows | implemented | Framed aggregates over `Window` | Boolean, statistical, and collection helpers are admitted; distinct windows stay unsupported by Spark | [Windows API](../api/Windows.api.md) |
| Partitioned `window_max` | implemented | Window aggregate over partition/order/frame | Explicit typed window validation keeps partitioned maximum compiler-visible | [Windows API](../api/Windows.api.md) |
| Raw `WindowSpec` escape hatch | unsupported | Direct PySpark `WindowSpec` | Use hooks for raw PySpark | [Windows API](../api/Windows.api.md) |

## Higher-Order And Collection Functions

Structure supports `arr_transform`, `arr_filter`, `arr_exists`, `arr_forall`, `arr_zip_with`, `arr_aggregate`,
`arr_sort_by`, `arr_flatten`, `arr_distinct`, `arr_position`, `map_transform_values`, `map_filter`,
`map_transform_keys`, `map_zip_with`, `map_keys`, `map_values`, `map_entries`, and `map_from_entries`.

| API / Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Collection size and membership | implemented | `size`, `cardinality`, `array_size`, `array_contains`, `map_contains_key` | Typed count and membership helpers preserve Spark null semantics | [Collections API](../api/Collections.api.md) |
| Array construction and set operations | implemented | `array`, `array_repeat`, `array_union`, `array_except`, `reduce` | Compatible numerics widen; other element types must agree; `reduce` requires a type-stable accumulator and symbolic callbacks | [Collections API](../api/Collections.api.md) |
| Array slicing and sorting variants | implemented | `slice`, `array_sort`, `sort_array`, `reverse` | `slice(...)`, `arr_sort(...)`, `sort_array(...)`, and `arr_reverse(...)` preserve typed array contracts | [Collections API](../api/Collections.api.md) |
| Array concatenation, joins, extrema, overlap, and shuffle | implemented | `concat`, `array_join`, `array_max`, `array_min`, `arrays_overlap`, `shuffle` | `concat` accepts homogeneous strings, binary values, or compatible arrays; joins require `array<string>`; extrema require orderable scalar elements; overlap propagates array/source nullability; shuffle preserves array typing without promising order | [Collections API](../api/Collections.api.md) |
| Element lookup and map concatenation | implemented | `get`, `element_at`, `try_element_at`, `map_concat` | `get` is zero-based; other array indexing is one-based; lookup results are nullable; map concat rejects duplicate-key policy overrides | [Collections API](../api/Collections.api.md) |
| `posexplode` over array of structs | implemented | `posexplode` | `posexplode_struct(...)` expands `array<struct>` with a declared generated scope | [Collections API](../api/Collections.api.md) |
| `explode`/`posexplode` over primitive arrays | implemented | `explode`, `explode_outer`, `posexplode`, `posexplode_outer` | Typed scalar-array generators require explicit value and ordinal field declarations | [Collections API](../api/Collections.api.md) |
| Typed struct generator forms | implemented | `explode`, outer generators, `inline` | Typed struct generator helpers define schema, cardinality, nullability, and streaming classification | [Collections API](../api/Collections.api.md) |
| Python control flow in callbacks | unsupported | Arbitrary Python lambdas | Return symbolic expressions only | [Collections API](../api/Collections.api.md) |

## Relation Operations

Relation operations change the active rowset's identity, cardinality, ordering, or available relation aliases. They are
Structure additions over public DataFrame transformation patterns, not raw DataFrame escape hatches.

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Set operations | implemented/design-gated | `union`, `unionByName`, `intersect`, `intersectAll`, `subtract`, `exceptAll` | Exact-schema relation set composition is implemented; batch `union_by_name(..., allow_missing_columns=True)` supports nullable fills, typed scalar defaults, nested struct paths, aliases, and explicit struct defaults | Streaming missing-column union remains design-gated; array/map element evolution is rejected |
| Branchable typed union | implemented | Union of compatible DataFrames | Independently materialized exact-schema lanes can converge through `union_all(...)` | Retired relevance-context expansion hooks |
| `relation_alias` self joins | implemented | DataFrame aliases for self joins | Named typed occurrence of the active rowset or an unjoined relation | [Joins API](../api/Joins.api.md) |
| Relation order/limit/offset | implemented | `orderBy`, `limit`, `offset` | Typed order descriptors and literal bounds; bounds require ordered current relation state | [API.md](../API.md#extensions-beyond-pyspark) |
| Range repartitioning | implemented | `repartitionByRange` | `repartition_by_range(count, *orderings)` requires a positive literal count and typed order expressions; batch-only and row-preserving | [Relations API](../api/Relations.api.md) |
| Relation coalescing | implemented | `DataFrame.coalesce` | `coalesce(partitions=...)` is row-preserving; streaming use emits a throughput advisory | [Relations API](../api/Relations.api.md) |
| `exactly_one` validation | implemented | Relation cardinality assertion | Declared assertion fails zero/multiple matches with `REL-E0701` | Retired Search query construction hook |
| `require_unique` / `require_all` / `require_reference` | implemented | Spark-plan assertions | Key, predicate, and nullable parent-reference checks fail through `REL-E0702`/`REL-E0703`/`REL-E0704` | [API.md](../API.md#extensions-beyond-pyspark) |
| Parent hierarchy validation | implemented | Finite DataFrame self-join validation | `require_parent_hierarchy(...)` checks missing parents, cycles, depth overruns, and child ordering with `REL-E0706` | [API.md](../API.md#extensions-beyond-pyspark) |
| First-qualified priority selection | implemented | Priority row selection pattern | `select_first_qualified(...)` selects at most one eligible row per key and reports `REL-E0705` for configured missing/tie failures | Retired document reranking hook |
| Parent hierarchy closure | implemented | Finite iterative self-join expansion | `hierarchy_closure(...)` replaces the active rowset with typed `(node, ancestor, depth)` rows up to literal `max_depth` | Retired cohort-band resolution hook |
| Bounded parent hierarchy fallbacks | implemented | Hierarchy expansion patterns | `hierarchy_fallbacks(...)` emits ordered parent-substitution fallback IDs plus the terminal global fallback row | Retired cohort-band resolution hook |
| Sampling | implemented | `sample` | Relation-level `sample(...)` requires a seed unless `reproducible=False`; streaming compatibility is batch-only | [API.md](../API.md#extensions-beyond-pyspark) |
| Bounded ordered `scan(...)` | implemented | Ordered recurrence pattern | Batch-only typed state recurrence over a caller-supplied, partitioned, ordered timeline with duplicate-key and bound checks | [Ordered Timeline Scan](../dev/specifications/OrderedTimelineScan.spec.md) |
| Matrix inversion | intentional raw | Driver-side numerical algorithm | Not a symbolic distributed DataFrame transformation | School example hook |

## Streaming

The streaming slice accepts compatible streaming DataFrames as inputs for row-local, watermarked stateful, and admitted
stream-stream operations. Structure intentionally does not own streaming lifecycle.

Use [`examples/streams/adoption.py`](../../examples/streams/adoption.py) as the tested caller-owned source/sink/query
lifecycle recipe.

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Generated streaming sources | unsupported | `spark.readStream` | Callers own source selection and configuration | [Streaming API](../api/Streaming.api.md) |
| Generated streaming sinks | unsupported | `DataFrame.writeStream` | Callers own sinks and side effects | [Streaming API](../api/Streaming.api.md) |
| Triggers, checkpoints, and output modes | unsupported | `trigger`, `checkpointLocation`, `outputMode` | Callers apply lifecycle policy; Structure may report required modes | [Streaming API](../api/Streaming.api.md) |
| Watermarks | implemented | `withWatermark` | Compiler-visible transform operation | [Streaming API](../api/Streaming.api.md) |
| Event-time tumbling and sliding aggregations | implemented | `groupBy(window(...))` | Requires a prior watermark on the direct event-time grouping key or `window(event_time, ...)`; caller uses `append` or `update` | [Streaming API](../api/Streaming.api.md) |
| Cross-mode dedupe | implemented | `dropDuplicates`, `dropDuplicatesWithinWatermark` | `drop_duplicates(...)` uses batch `dropDuplicates` and streaming bounded dedupe after a watermark | [Streaming API](../api/Streaming.api.md) |
| Explicit bounded dedupe | implemented | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(...)` requires `streaming=True` and a preceding watermark | [Streaming API](../api/Streaming.api.md) |
| Session-window aggregation | implemented | `session_window` | Requires a preceding watermark on the event-time field, a static positive gap, at least one ordinary grouping key, and caller-owned `append` mode | [Streaming API](../api/Streaming.api.md) |
| Chained window aggregation | implemented | `window_time`, `window(window_time(...))` | Exactly one watermarked event-time window aggregate followed by a second window aggregate; PySpark 3.5/4.0 online and generated evidence passes | [Streaming API](../api/Streaming.api.md) |
| Variant row-local helpers | implemented | `parse_json`, `schema_of_variant`, `variant_get`, `to_variant_object`, `is_variant_null`, `variant_literal`, `variant_explode`, `variant_explode_outer` | Ordinary PySpark 4 profile-gated streaming transforms; PySpark 4.0 has live online/generated evidence for validated literals, watermarked schema aggregation, and both TVF forms, including outer null-row and object-key contracts, and PySpark 3.5 fails before execution | [Streaming API](../api/Streaming.api.md) |
| Bounded stream-stream outer and semi joins | implemented | Left/right/full outer and left-semi stream-stream joins | Requires declared streaming inputs, watermarks on both bound event-time fields, a compiler-visible event-time bound, and caller-owned `append` mode | [Streaming API](../api/Streaming.api.md) |
| Stream-static left semi join | implemented | Left-semi stream-static join | Non-stateful `exists(...)` filter when the active input is streaming and the right input is static | [Streaming API](../api/Streaming.api.md) |
| Unsupported stream-static directions | unsupported | Right/full/cross/anti stream-static joins | These runtime shapes are not admitted by Spark Structured Streaming | Use supported left/inner/left-semi lookup or caller-owned redesign |
| Global/unbounded aggregation and dedupe | unsupported | Global `groupBy`, unwatermarked `dropDuplicates` | Structure will not admit unbounded state | Group by watermarked event time/window or bound state outside Structure |
| Global ordering, limits, and offsets | streaming-ineligible | `orderBy`, `sort`, `limit`, `offset` | These are batch-materialization boundaries over unbounded streams | Use caller-owned PySpark after a materialization boundary |
| Relation coalescing | implemented | `DataFrame.coalesce` | `coalesce(partitions=...)` preserves rows; streaming use emits the `STREAM-W0803` throughput advisory | [Relations API](../api/Relations.api.md) |
| Priority selection | streaming-ineligible | `select_first_qualified`, top-N | Lowers through ranking and validation aggregates; remains batch-only | Use caller-owned PySpark after a materialization boundary |
| Analytic windows and selected-row helpers | streaming-ineligible | ranking, `Window`, lag/lead, rolling windows, latest/earliest | Broad analytic projections and global selected-row helpers have no finite streaming state contract; grouped `first_value(...)`/`last_value(...)` inside a watermarked event-time window is the admitted finite alternative | Use the finite grouped aggregate or caller-owned PySpark after materialization |
| Stateful composition boundary | implemented | One streaming aggregate/dedupe/join followed by stateless operations | The one-stateful-plus-stateless policy rejects a second stateful operation with diagnostics | [Streaming API](../api/Streaming.api.md) |
| Chained stateful operators | design-gated | Chains of streaming aggregates/dedupe/joins | Needs explicit composition and state-budget policy before Structure can own the shape | Use caller-owned PySpark |
| Pandas and RDD boundaries | unsupported | Pandas UDF, RDD, `mapInPandas` | Opaque execution does not fit Structure's symbolic transform contract | Use caller-owned streaming code |
| Arbitrary state processors | design-gated | `applyInPandasWithState`, `transformWithState` | `ArbitraryStateContract` validates the typed state boundary, but does not provide a runtime or recovery guarantee | Use caller-owned state code only after recording the contract and live restart evidence |
| Typed struct generators | implemented | `stack`, `explode`, `posexplode`, `inline` | Fixed `stack` and typed array-of-struct generators are stateless row expansion with schema/cardinality contracts | [Collections API](../api/Collections.api.md) |
| Caller-owned lifecycle APIs | caller-owned-guided | Sources, sinks, triggers, checkpoints, query start/stop | Structure only transforms supplied DataFrames; executable recipes keep lifecycle outside generated modules | [Streaming API](../api/Streaming.api.md) |
| `foreachBatch` side-effect sinks | caller-owned-guided | `DataStreamWriter.foreachBatch` | Use `examples.streams.adoption.start_foreach_batch_query(...)` with `ForeachBatchSafety` after Structure returns a transformed DataFrame; the helper validates sink identity, idempotence key, retry policy, and snapshot identity before start | [Streaming API](../api/Streaming.api.md) |
| Row-level `foreach` sinks | design-gated | `DataStreamWriter.foreach` | Needs sink identity, idempotence, retry, and recovery contracts before any Structure-owned support | [Spark Streaming](../dev/specifications/SparkStreaming.spec.md) |

## API Coverage

This section is Structure's checked catalog for its PySpark `>=3.5,<4.1` transformation baseline. It covers typed
transformations over caller-supplied DataFrames, not readers, writers, sessions, catalog/table management, actions, or
streaming lifecycle. The catalog below is the public summary of the current compatibility decisions; focused API
references provide the user-facing operation details.

| PySpark family | Status | Structure spelling or alternative | Contract / notes |
| --- | --- | --- | --- |
| Column comparisons, boolean, arithmetic | supported | Symbolic operators, `between`, `isin`, `isNaN`, null predicates | Typed expressions preserve rows; `isNaN` is exposed as `isnan()`, and `isin` supports variadic values or one list. |
| Column bitwise operations | supported | `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` | Integer/long only; preserves rows. |
| Column string predicates | supported | `contains`, `startswith`, `endswith`, `like`, `ilike`, `rlike` | Typed String predicates preserve rows; `contains`, `startswith`, and `endswith` accept literal or String-expression operands. |
| Column cast and nested access | supported | `cast`, `try_cast`, attributes, `get_field`, indexing | `try_cast` is capability checked. |
| Struct mutation | supported | `with_field`, `drop_fields` | Requires exact declared struct shape. |
| Column alias and raw `over` | unsupported | Schema fields; typed window helpers | Names and window contracts remain compiler-visible. |
| Conditional/null/string/numeric/temporal functions | supported | `when`, `coalesce`, `nullif`, typed scalar helpers | Exact type and nullability semantics are compiler-visible. |
| Hash and encoding | supported | Typed hashes, `base64`, `unbase64`, `encode`, `decode`, `to_binary`, `try_to_binary` | Hashes and binary encoding helpers are typed scalar expressions. |
| JSON/CSV conversion | supported | `from_json`, `to_json`, `from_csv`, `to_csv` | Schema-carrying parsing keeps parser options and output schemas compiler-visible. |
| Array construction, lookup, transformation | supported | Typed array helpers including `concat`, `shuffle`, `reduce`, and `arrays_zip` | Exact element/nullability, nondeterminism, array-shape, and callback rules are validated. |
| Map functions | supported | Typed map helpers | Callback bodies remain symbolic. |
| Generator variants | supported | `explode_struct`, `explode_outer_struct`, `posexplode_struct`, `posexplode_outer_struct`, `inline_struct`, `inline_outer_struct`, `explode_array`, `explode_outer_array`, `posexplode_array`, `posexplode_outer_array`, `explode_map`, `explode_outer_map`, `posexplode_map`, `posexplode_outer_map` | Typed struct, primitive scalar-array, and primitive map generators expand declared values with schema/cardinality contracts. |
| Projection and filtering | supported | Schema projection and `where` | Schema owns output names and replacement. |
| Joins and hints | supported | Typed join helpers, `relation_alias` | Explicit schema/cardinality; cross needs opt-in; self joins require named aliases. |
| Set operations | supported/design-gated | `union_all`, `union_by_name`, `intersect`, `intersect_all`, `subtract`, `except_all`; nullable missing-column `union_by_name` | Exact-schema set operations are supported; batch `allow_missing_columns=True` fills nullable or explicitly defaulted top-level and nested struct fields while preserving aliases. | Streaming missing-column union remains design-gated; array/map element evolution is rejected. |
| Ordering/limit/sample | supported | `order_by`, `limit`, `offset`, `sample` | Ordered bounds are compiler-visible and batch-only; sampling requires explicit reproducibility policy and is batch-only. |
| Priority selection | supported | `select_first_qualified` | Declared business keys, eligibility, and priority order select one row per key; configured missing/tie failures report `REL-E0705`. |
| Distinct and deduplication | supported | `distinct`, `drop_duplicates` | Watermark form is streaming classified. |
| Grouping and standard aggregates | supported | `group_by`, `rollup`, `cube`, typed aggregates | Declared aggregate output schema. |
| Opaque sketches/bitmaps | supported | HLL construction, pairwise/aggregate union, estimates, Bitmap construction/OR, and counts | Branded; no Binary interchange. |
| Exact percentile and statistics | mixed | `percentile`, approximate and moment helpers, `mode(...)` | Grouped `mode(value, deterministic=False)` uses Spark 4 native deterministic mode and an equivalent typed Spark 3.5 lowering when `deterministic=True`. |
| Ranking, selection, aggregate windows | supported | Typed window helpers | Raw `WindowSpec` is unsupported. |
| Watermarks | supported | `watermark` | Caller owns source, sink, trigger, output mode, and lifecycle. |
| Session window | supported | `session_window(event_time, gap)` | Static positive gap returns a typed `TimeWindow` grouping key. |
| Bounded stream-stream outer/semi joins | supported | `rowset_join(..., how="left"|RIGHT|FULL)`, `exists(...)` | Both streams require watermarks and `event_time_between(...)`; caller uses append mode. |
| Stream-static semi filtering | supported | `exists(...)` | The streaming relation stays on the left; it has no state or output-mode requirement. |

Excluded categories stay caller-owned: readers, writers, storage, catalogs, sessions, table management, actions,
materializers, streaming lifecycle operations, Python UDTFs, Pandas APIs, RDD APIs, and arbitrary callback APIs.
Existing scalar `@special(type="udf")` remains its documented ordinary-PySpark exception.

## Reference

[API.ref.md](../reference/API.ref.md).
