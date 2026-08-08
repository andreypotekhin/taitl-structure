# API Catalog

This catalog is the reference for API compatibility decisions. Use `@raw` or caller-owned PySpark if it says the symbolic contract is design-gated, streaming-ineligible, or unsupported.

For extensions on top of PySpark, see [APIExtensions.md](APIExtensions.md). For Structure own APIs such as schemas, transforms, hooks, see Core APIs in [API.md](API.md).

Planning for remaining actionable rows is grouped in the
[API catalog and schema-evolution plan](dev/planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md).
Companion streaming state, side-effect, and evidence plans are linked from the
[streaming project plan](dev/project-management/V10.md). An open or gated row is not a support claim;
each entry must name its owner boundary, evidence, and caller remedy.

## PySpark 4.1 adoption

The adoption work adds a separate ledger for the PySpark `>=4.1,<4.2` profile. These rows are admission classifications,
not current support claims. The primary target variant is ordinary PySpark; Spark Connect receives a support claim only
when its 4.1-specific evidence passes. The governing design and specification are
[PySpark 4.1 design](dev/design/V11PySpark41Adoption.design.md) and
[PySpark 4.1 parity specification](dev/specifications/V11PySpark41Parity.spec.md).

| PySpark 4.1 addition | Status | Structure boundary | Design evidence or remedy |
| --- | --- | --- | --- |
| `Column.transform` and new higher-order column operations | design-gated | Typed symbolic element callback with declared result type; row-preserving array transformation only | Expression design, nullability/type tests, online/generated ordinary 4.1 parity, then Connect parity if documented |
| New deterministic scalar, string, binary, temporal, and collection functions | design-gated | Admit only functions with explicit type, nullability, generated spelling, and streaming contracts | 4.0-to-4.1 inventory diff and one capability/test/evidence row per function family |
| Random and seeded helpers such as `random`, `uniform`, `randstr`, and `uuid` | design-gated | Requires an explicit seed and nondeterminism policy; no silent streaming claim | Use a caller-owned PySpark expression or wait for the expression design gate |
| `DataFrame.exists` and IN-subquery operations | design-gated | Correlated boolean relation predicate with explicit aliases and null semantics | Query design, duplicate/empty/null/correlation tests, explain traceability, ordinary and proven Connect evidence |
| `DataFrame.lateralJoin` | design-gated | Requires typed output schema, correlation scope, cardinality, and streaming classification | Use caller-owned PySpark until the typed relation contract is implemented |
| Complex-valued `DataFrame.observe` metrics | design-gated | Observation is a metric side channel, not an implicit output-field mutation | Use caller-owned observation hooks until metric types, retrieval, and parity are specified |
| KLL and Theta approximate-sketch aggregates | design-gated | Requires binary result, mergeability, precision, dependency, and determinism contracts | Use caller-owned PySpark or ordinary aggregate alternatives |
| Arrow-optimized Python UDF/UDTF APIs | caller-owned-guided | Arbitrary worker Python and UDTF cardinality remain outside the symbolic compiler contract | Use an explicit raw/caller-owned hook; no generated UDF/UDTF claim |
| Row-based `transformWithState` | design-gated | User-owned state, timers, recovery, and streaming lifecycle remain outside Structure | Use caller-owned Structured Streaming state code |
| Declarative Pipelines, SQL Scripting, Python Data Sources, readers/writers, and catalog/session APIs | unsupported | Not compiler-visible DataFrame transformations | Use native PySpark/Spark orchestration around Structure |

The 4.1 target is intentionally additive: the existing `>=3.5,<4.1` catalog remains the current baseline until the
adoption closeout promotes the default. Every row above must move to a final status with a capability key, diagnostic,
specification, focused test, and evidence path before release.

## Column API

Structure supports typed field references, nested struct field access, equality and ordering comparisons, boolean
composition, arithmetic `+`, `-`, and `*`, null predicates, `isin(...)`, and inclusive `between(...)`.

| Capability | Status | PySpark parity | Structure contract | Reference |
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

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Broader string helpers | implemented | `ltrim`, `rtrim`, `substring`, `split`, `regexp_replace`, `regexp_extract`, `length`, `concat_ws`, `initcap`, `reverse`, `translate`, `instr`, `levenshtein` | Typed cross-version string transformation, search, comparison, and `array<string>` path joining core | [Expressions API](api/Expressions.api.md) |
| Date/time helpers | implemented | `date_add`, `date_sub`, `datediff`, `date_trunc`, `trunc`, calendar extraction, `to_date`, `to_timestamp` | Typed Date/Timestamp temporal helper set | [Expressions API](api/Expressions.api.md) |
| Numeric/math helpers | implemented | `abs`, `round`, `bround`, `ceil`, `floor`, `sqrt`, `pow`, `log`, `exp`, `signum` | Typed deterministic scalar helper set | [Expressions API](api/Expressions.api.md) |
| Predicate helpers | implemented | `isnull`, `isnotnull`, `isnan` | Function-style null checks and typed NaN predicate | [Expressions API](api/Expressions.api.md) |
| Null-control functions | implemented | `nullif`, `nvl`, `nvl2`, `ifnull`, `zeroifnull`, `nanvl` | Typed fallback, branch, null, and NaN semantics | [Expressions API](api/Expressions.api.md) |
| Hash helpers | implemented | `hash`, `xxhash64`, `md5`, `sha1`, `sha2` | Typed scalar hashes and string digests; not security or cross-engine identity primitives | [Expressions API](api/Expressions.api.md) |
| Encoding/binary helpers | implemented | `base64`, `unbase64`, `encode`, `decode` | Binary helpers use the public binary field type and typed scalar lowering | [Expressions API](api/Expressions.api.md) |
| JSON/CSV helpers | implemented | `from_json`, `to_json`, `from_csv`, `to_csv` | Schema-carrying parsing keeps data contracts compiler-visible | [Expressions API](api/Expressions.api.md) |
| XML helpers | design-gated | Spark XML functions | Low-priority schema-carrying parser/generator design; XML remains outside the active implementation path | [API Catalog Design Gates](dev/design/ApiCatalogDesignGates.design.md) |
| Variant field | implemented | `VariantType` | `variant(...)` preserves an opaque Spark Variant value in schemas; it requires a resolved PySpark 4 profile | [Schemas API](api/Schemas.api.md) |
| Variant helpers | implemented | `parse_json`, `try_parse_json`, `variant_literal`, `variant_get`, `try_variant_get`, `schema_of_variant`, `schema_of_variant_agg`, `to_variant_object`, `is_variant_null`, `is_valid_variant`, `variant_explode`, `variant_explode_outer` | Typed strict/safe parsing, validated literals, extraction, schema inspection, conversion, JSON-null testing, equality, validation, and PySpark 4 TVF row expansion. `is_valid_variant(...)` requires `>=4.2,<4.3`. | [Expressions API](api/Expressions.api.md) |
| Variant mutation helpers | design-gated | `variant_array_append`, `try_variant_array_append`, `variant_insert`, `try_variant_insert`, `variant_set`, `try_variant_set`, `variant_delete` | Reserved for PySpark 4.3+ profiles, which are not yet released in the supported project matrix; current PySpark 4.2 transforms reject these helpers before lowering. | [API Catalog Design Gates](dev/design/ApiCatalogDesignGates.design.md) |
| Geospatial helpers | design-gated | Provider-neutral Spark SQL `GEOMETRY`/`ST_*` contract | The bundled DSL admits a narrow typed geometry slice; runtime providers remain optional and provider-specific | [API Catalog Design Gates](dev/design/ApiCatalogDesignGates.design.md) |
| Scalar Python UDFs | implemented | PySpark `udf`; `@special(type="udf")` | Ordinary PySpark and Spark Connect row-local batch support with warning policy; streaming remains ordinary-only | Public Spark Connect UDF API |
| Python UDTFs and UDTs | unsupported | `udtf`, UDT | Row expansion and custom type semantics are caller-owned | Use caller-owned PySpark or hooks |
| Raw SQL string expressions | unsupported | `expr`, `call_function` | Compiler-visible expressions stay structured | Use typed helpers or hooks |

## Joins

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Using-key joins | implemented | `join(on="key")`, `on=["k1", "k2"]` | Symbolic `on=` remains preferred | [Joins API](api/Joins.api.md) |
| Full join diagnostics hardening | implemented | `how="full"` | Nullable sides are named clearly | [Joins API](api/Joins.api.md) |
| Right join diagnostics hardening | implemented | `how="right"` | Rowset API exists; projection rules stay explicit | [Joins API](api/Joins.api.md) |
| Cross join safety | implemented | `crossJoin`, `how="cross"` | Requires `allow_cartesian=True` | [Joins API](api/Joins.api.md) |
| Join strategy directives | implemented | `broadcast`, `merge`, shuffle hints | Capability-checked PySpark hints | [Joins API](api/Joins.api.md) |
| Join reordering | design-gated | Cost-based join planning | No public `join_order(...)` in the current profile; logical reordering needs dependency-safe predicate analysis and explainable selected order | [API Catalog Design Gates](dev/design/ApiCatalogDesignGates.design.md) |
| Backward/forward as-of joins | implemented | Directional as-of matching | Selects the latest previous or earliest following qualifying right row | [Joins API](api/Joins.api.md) |
| Nearest as-of joins | implemented | Nearest time matching | Selects the closest non-null right time and fails equidistant matches with `ties="error"` | [Joins API](api/Joins.api.md) |
| Unbounded or non-contract stream-stream joins | unsupported | Streaming stream-stream joins | Only admitted bounded forms are allowed; all need input modes, watermarks, event-time bounds, and state diagnostics | [Streaming API](api/Streaming.api.md) |
| Raw SQL join predicates | unsupported | SQL strings in `on` | Use symbolic expressions or hooks | [Joins API](api/Joins.api.md) |

## Aggregations

Structure supports ordinary grouping, rollup, cube, explicit grouping sets, `having(...)`, common exact aggregates,
approximate count/percentile, boolean aggregates, statistical aggregates, filtered metrics, collection aggregates, and
deterministic first/last helpers.

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Explicit grouping sets | implemented | Custom grouping-set levels | Lowers as generated PySpark branch unions | [Aggregations API](api/Aggregations.api.md) |
| Having predicates | implemented | SQL/PySpark post-aggregate filters | Uses aggregate-output predicate scope | [Aggregations API](api/Aggregations.api.md) |
| Implicit global aggregation | implemented | Global aggregate without grouping keys | Aggregate-only steps retain global semantics and enforce empty-input nullability | [Aggregations API](api/Aggregations.api.md) |
| Ordered `collect_list` | implemented | Ordered collection aggregate | Explicit ascending/descending aggregate keys preserve deterministic collection order | [Aggregations API](api/Aggregations.api.md) |
| Aggregate aliases | unsupported | `GroupedData.agg` aliases | Output schema constructors and field `alias=...` own aggregate names; no second aggregate aliasing API | [Aggregations API](api/Aggregations.api.md) |
| Exact percentile family | implemented | `percentile`, `percentile_approx` | `percentile(...)` uses scalar 0-1 percentage and positive literal frequency; `approx_percentile(...)` is bounded-memory | [Aggregations API](api/Aggregations.api.md) |
| Additional stats | implemented | `skewness`, `kurtosis`, `mode` | `mode(value, deterministic=False)` uses PySpark 4 native deterministic mode and an equivalent typed PySpark 3.5 lowering when `deterministic=True` | [Aggregations API](api/Aggregations.api.md) |
| Deterministic selected-row helpers | implemented | Ordered aggregate/window selection patterns | `earliest_by`, `latest_by`, `dedupe_earliest_by`, and `dedupe_latest_by` encode deterministic row-selection policy | [Aggregations API](api/Aggregations.api.md) |
| Dict/list aggregate syntax | unsupported | `GroupedData.agg({"x": "sum"})` | Use typed helpers | [Aggregations API](api/Aggregations.api.md) |

## Windows

Structure supports inline ranking/lag/lead/rolling helpers and reusable window specs with explicit row/range frames.

| API / Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Null ordering in window order keys | implemented | Null-ordering sort methods | Typed order descriptors render in inline and reusable windows | [Windows API](api/Windows.api.md) |
| Multiple order keys in all helpers | implemented | `Window.orderBy(*cols)` | Inline and reusable helpers preserve ordered keys | [Windows API](api/Windows.api.md) |
| Additional aggregate windows | implemented | Framed aggregates over `Window` | Boolean, statistical, and collection helpers are admitted; distinct windows stay unsupported by Spark | [Windows API](api/Windows.api.md) |
| Partitioned `window_max` | implemented | Window aggregate over partition/order/frame | Explicit typed window validation keeps partitioned maximum compiler-visible | [Windows API](api/Windows.api.md) |
| Raw `WindowSpec` escape hatch | unsupported | Direct PySpark `WindowSpec` | Use hooks for raw PySpark | [Windows API](api/Windows.api.md) |

## Higher-Order And Collection Functions

Structure supports `arr_transform`, `arr_filter`, `arr_exists`, `arr_forall`, `arr_zip_with`, `arr_aggregate`,
`arr_sort_by`, `arr_flatten`, `arr_distinct`, `arr_position`, `map_transform_values`, `map_filter`,
`map_transform_keys`, `map_zip_with`, `map_keys`, `map_values`, `map_entries`, and `map_from_entries`.

| API / Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Collection size and membership | implemented | `size`, `array_contains`, `map_contains_key` | Typed count and membership helpers preserve Spark null semantics | [Collections API](api/Collections.api.md) |
| Array construction and set operations | implemented | `array`, `array_repeat`, `array_union`, `array_except` | Compatible numerics widen; other element types must agree | [Collections API](api/Collections.api.md) |
| Array slicing and sorting variants | implemented | `slice`, `array_sort`, `reverse` | `slice(...)`, `arr_sort(...)`, and `arr_reverse(...)` preserve typed array contracts | [Collections API](api/Collections.api.md) |
| Element lookup and map concatenation | implemented | `element_at`, `try_element_at`, `map_concat` | Lookup results are nullable; safe lookup avoids out-of-range errors; map concat rejects duplicate-key policy overrides | [Collections API](api/Collections.api.md) |
| `posexplode` over array of structs | implemented | `posexplode` | `posexplode_struct(...)` expands `array<struct>` with a declared generated scope | [Collections API](api/Collections.api.md) |
| `explode`/`posexplode` over primitive arrays | implemented | `explode`, `explode_outer`, `posexplode`, `posexplode_outer` | Typed scalar-array generators require explicit value and ordinal field declarations | [Collections API](api/Collections.api.md) |
| Typed struct generator forms | implemented | `explode`, outer generators, `inline` | Typed struct generator helpers define schema, cardinality, nullability, and streaming classification | [Collections API](api/Collections.api.md) |
| Python control flow in callbacks | unsupported | Arbitrary Python lambdas | Return symbolic expressions only | [Collections API](api/Collections.api.md) |

## Relation Operations

Relation operations change the active rowset's identity, cardinality, ordering, or available relation aliases. They are
Structure additions over public DataFrame transformation patterns, not raw DataFrame escape hatches.

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Set operations | implemented/design-gated | `union`, `unionByName`, `intersect`, `intersectAll`, `subtract`, `exceptAll` | Exact-schema relation set composition is implemented; batch `union_by_name(..., allow_missing_columns=True)` supports nullable fills, typed scalar defaults, nested struct paths, aliases, and explicit struct defaults | Streaming missing-column union remains design-gated; array/map element evolution is rejected |
| Branchable typed union | implemented | Union of compatible DataFrames | Independently materialized exact-schema lanes can converge through `union_all(...)` | Retired relevance-context expansion hooks |
| `relation_alias` self joins | implemented | DataFrame aliases for self joins | Named typed occurrence of the active rowset or an unjoined relation | [Joins API](api/Joins.api.md) |
| Relation order/limit/offset | implemented | `orderBy`, `limit`, `offset` | Typed order descriptors and literal bounds; bounds require ordered current relation state | [APIExtensions.md](APIExtensions.md#added-relation-helpers) |
| `exactly_one` validation | implemented | Relation cardinality assertion | Declared assertion fails zero/multiple matches with `REL-E0701` | Retired Search query construction hook |
| `require_unique` / `require_all` / `require_reference` | implemented | Spark-plan assertions | Key, predicate, and nullable parent-reference checks fail through `REL-E0702`/`REL-E0703`/`REL-E0704` | [APIExtensions.md](APIExtensions.md) |
| Parent hierarchy validation | implemented | Finite DataFrame self-join validation | `require_parent_hierarchy(...)` checks missing parents, cycles, depth overruns, and child ordering with `REL-E0706` | [APIExtensions.md](APIExtensions.md) |
| First-qualified priority selection | implemented | Priority row selection pattern | `select_first_qualified(...)` selects at most one eligible row per key and reports `REL-E0705` for configured missing/tie failures | Retired document reranking hook |
| Parent hierarchy closure | implemented | Finite iterative self-join expansion | `hierarchy_closure(...)` replaces the active rowset with typed `(node, ancestor, depth)` rows up to literal `max_depth` | Retired cohort-band resolution hook |
| Bounded parent hierarchy fallbacks | implemented | Hierarchy expansion patterns | `hierarchy_fallbacks(...)` emits ordered parent-substitution fallback IDs plus the terminal global fallback row | Retired cohort-band resolution hook |
| Sampling | implemented | `sample` | Relation-level `sample(...)` requires a seed unless `reproducible=False`; streaming compatibility is batch-only | [APIExtensions.md](APIExtensions.md#added-relation-helpers) |
| Bounded ordered `scan(...)` | implemented | Ordered recurrence pattern | Batch-only typed state recurrence over a caller-supplied, partitioned, ordered timeline with duplicate-key and bound checks | [Ordered Timeline Scan](dev/specifications/OrderedTimelineScan.spec.md) |
| Matrix inversion | intentional raw | Driver-side numerical algorithm | Not a symbolic distributed DataFrame transformation | School example hook |

## Streaming

The streaming slice accepts compatible streaming DataFrames as inputs for row-local, watermarked stateful, and admitted
stream-stream operations. Structure intentionally does not own streaming lifecycle.

The checked streaming inventory lives in
[`pyspark-streaming-api-coverage.json`](../src/structure/plugin/pyspark/resources/pyspark-streaming-api-coverage.json).
Use [`examples/streams/adoption.py`](../examples/streams/adoption.py) as the tested caller-owned source/sink/query
lifecycle recipe.

| Capability | Status | PySpark parity | Structure contract | Reference |
| --- | --- | --- | --- | --- |
| Generated streaming sources | unsupported | `spark.readStream` | Callers own source selection and configuration | [Streaming API](api/Streaming.api.md) |
| Generated streaming sinks | unsupported | `DataFrame.writeStream` | Callers own sinks and side effects | [Streaming API](api/Streaming.api.md) |
| Triggers, checkpoints, and output modes | unsupported | `trigger`, `checkpointLocation`, `outputMode` | Callers apply lifecycle policy; Structure may report required modes | [Streaming API](api/Streaming.api.md) |
| Watermarks | implemented | `withWatermark` | Compiler-visible transform operation | [Streaming API](api/Streaming.api.md) |
| Event-time tumbling and sliding aggregations | implemented | `groupBy(window(...))` | Requires a prior watermark on the direct event-time grouping key or `window(event_time, ...)`; caller uses `append` or `update` | [Streaming API](api/Streaming.api.md) |
| Cross-mode dedupe | implemented | `dropDuplicates`, `dropDuplicatesWithinWatermark` | `drop_duplicates(...)` uses batch `dropDuplicates` and streaming bounded dedupe after a watermark | [Streaming API](api/Streaming.api.md) |
| Explicit bounded dedupe | implemented | `dropDuplicatesWithinWatermark` | `drop_duplicates_within_watermark(...)` requires `streaming=True` and a preceding watermark | [Streaming API](api/Streaming.api.md) |
| Session-window aggregation | implemented | `session_window` | Requires a preceding watermark on the event-time field, a static positive gap, at least one ordinary grouping key, and caller-owned `append` mode | [Streaming API](api/Streaming.api.md) |
| Chained window aggregation | implemented | `window_time`, `window(window_time(...))` | Exactly one watermarked event-time window aggregate followed by a second window aggregate; PySpark 3.5/4.0 online and generated evidence passes | [Streaming API](api/Streaming.api.md) |
| Variant row-local helpers | implemented | `parse_json`, `schema_of_variant`, `variant_get`, `to_variant_object`, `is_variant_null`, `variant_literal`, `variant_explode`, `variant_explode_outer` | Ordinary PySpark 4 profile-gated streaming transforms; PySpark 4.0 has live online/generated evidence for validated literals, watermarked schema aggregation, and both TVF forms, including outer null-row and object-key contracts, and PySpark 3.5 fails before execution | [Streaming API](api/Streaming.api.md) |
| Bounded stream-stream outer and semi joins | implemented | Left/right/full outer and left-semi stream-stream joins | Requires declared streaming inputs, watermarks on both bound event-time fields, a compiler-visible event-time bound, and caller-owned `append` mode | [Streaming API](api/Streaming.api.md) |
| Stream-static left semi join | implemented | Left-semi stream-static join | Non-stateful `exists(...)` filter when the active input is streaming and the right input is static | [Streaming API](api/Streaming.api.md) |
| Unsupported stream-static directions | unsupported | Right/full/cross/anti stream-static joins | These runtime shapes are not admitted by Spark Structured Streaming | Use supported left/inner/left-semi lookup or caller-owned redesign |
| Global/unbounded aggregation and dedupe | unsupported | Global `groupBy`, unwatermarked `dropDuplicates` | Structure will not admit unbounded state | Group by watermarked event time/window or bound state outside Structure |
| Global ordering, limits, and offsets | streaming-ineligible | `orderBy`, `sort`, `limit`, `offset` | These are batch-materialization boundaries over unbounded streams | Use caller-owned PySpark after a materialization boundary |
| Priority selection | streaming-ineligible | `select_first_qualified`, top-N | Lowers through ranking and validation aggregates; remains batch-only | Use caller-owned PySpark after a materialization boundary |
| Analytic windows and selected-row helpers | streaming-ineligible | ranking, `Window`, lag/lead, rolling windows, latest/earliest | Broad analytic projections and global selected-row helpers have no finite streaming state contract; grouped `first_value(...)`/`last_value(...)` inside a watermarked event-time window is the admitted finite alternative | Use the finite grouped aggregate or caller-owned PySpark after materialization |
| Stateful composition boundary | implemented | One streaming aggregate/dedupe/join followed by stateless operations | The one-stateful-plus-stateless policy rejects a second stateful operation with diagnostics | [Streaming API](api/Streaming.api.md) |
| Chained stateful operators | design-gated | Chains of streaming aggregates/dedupe/joins | Needs explicit composition and state-budget policy before Structure can own the shape | Use caller-owned PySpark |
| Pandas and RDD boundaries | unsupported | Pandas UDF, RDD, `mapInPandas` | Opaque execution does not fit Structure's symbolic transform contract | Use caller-owned streaming code |
| Arbitrary state processors | design-gated | `applyInPandasWithState`, `transformWithState` | `ArbitraryStateContract` validates the typed state boundary, but does not provide a runtime or recovery guarantee | Use caller-owned state code only after recording the contract and live restart evidence |
| Typed struct generators | implemented | `explode`, `posexplode`, `inline` | Typed array-of-struct generators are admitted as stateless row expansion with schema/cardinality contracts | [Collections API](api/Collections.api.md) |
| Caller-owned lifecycle APIs | caller-owned-guided | Sources, sinks, triggers, checkpoints, query start/stop | Structure only transforms supplied DataFrames; executable recipes keep lifecycle outside generated modules | [Streaming API](api/Streaming.api.md) |
| `foreachBatch` side-effect sinks | caller-owned-guided | `DataStreamWriter.foreachBatch` | Use `examples.streams.adoption.start_foreach_batch_query(...)` with `ForeachBatchSafety` after Structure returns a transformed DataFrame; the helper validates sink identity, idempotence key, retry policy, and snapshot identity before start | [Streaming API](api/Streaming.api.md) |
| Row-level `foreach` sinks | design-gated | `DataStreamWriter.foreach` | Needs sink identity, idempotence, retry, and recovery contracts before any Structure-owned support | [Spark Streaming](dev/specifications/SparkStreaming.spec.md) |

## API Coverage

This section is Structure's checked catalog for its PySpark `>=3.5,<4.1` transformation baseline. It covers typed
transformations over caller-supplied DataFrames, not readers, writers, sessions, catalog/table management, actions, or
streaming lifecycle. The companion streaming API ledger classifies PySpark Structured Streaming adoption APIs,
stateful operations, lifecycle boundaries, and design-gated/unsupported families. The machine-checked inventory and entries
remain in
[`pyspark-transformation-inventory.json`](../src/structure/plugin/pyspark/resources/pyspark-transformation-inventory.json)
and
[`pyspark-transformation-coverage.json`](../src/structure/plugin/pyspark/resources/pyspark-transformation-coverage.json);
streaming-specific rows remain in
[`pyspark-streaming-api-coverage.json`](../src/structure/plugin/pyspark/resources/pyspark-streaming-api-coverage.json).

| PySpark family | Status | Structure spelling or alternative | Contract / notes |
| --- | --- | --- | --- |
| Column comparisons, boolean, arithmetic | supported | Symbolic operators, `between`, `isin`, null predicates | Typed expressions preserve rows. |
| Column bitwise operations | supported | `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` | Integer/long only; preserves rows. |
| Column string predicates | supported | `contains`, `startswith`, `endswith`, `like`, `ilike`, `rlike` | Typed String predicates preserve rows. |
| Column cast and nested access | supported | `cast`, `try_cast`, attributes, `get_field`, indexing | `try_cast` is capability checked. |
| Struct mutation | supported | `with_field`, `drop_fields` | Requires exact declared struct shape. |
| Column alias and raw `over` | unsupported | Schema fields; typed window helpers | Names and window contracts remain compiler-visible. |
| Conditional/null/string/numeric/temporal functions | supported | `when`, `coalesce`, `nullif`, typed scalar helpers | Exact type and nullability semantics are compiler-visible. |
| Hash and encoding | supported | Typed hashes, `base64`, `unbase64`, `encode`, `decode` | Hashes and binary encoding helpers are typed scalar expressions. |
| JSON/CSV conversion | supported | `from_json`, `to_json`, `from_csv`, `to_csv` | Schema-carrying parsing keeps parser options and output schemas compiler-visible. |
| Array construction, lookup, transformation | supported | Typed array helpers | Exact element/nullability and callback rules are validated. |
| Map functions | supported | Typed map helpers | Callback bodies remain symbolic. |
| Generator variants | supported | `explode_struct`, `explode_outer_struct`, `posexplode_struct`, `posexplode_outer_struct`, `inline_struct`, `inline_outer_struct`, `explode_array`, `explode_outer_array`, `posexplode_array`, `posexplode_outer_array`, `explode_map`, `explode_outer_map`, `posexplode_map`, `posexplode_outer_map` | Typed struct, primitive scalar-array, and primitive map generators expand declared values with schema/cardinality contracts. |
| Projection and filtering | supported | Schema projection and `where` | Schema owns output names and replacement. |
| Joins and hints | supported | Typed join helpers, `relation_alias` | Explicit schema/cardinality; cross needs opt-in; self joins require named aliases. |
| Set operations | supported/design-gated | `union_all`, `union_by_name`, `intersect`, `intersect_all`, `subtract`, `except_all`; nullable missing-column `union_by_name` | Exact-schema set operations are supported; batch `allow_missing_columns=True` fills nullable or explicitly defaulted top-level and nested struct fields while preserving aliases. | Streaming missing-column union remains design-gated; array/map element evolution is rejected. |
| Ordering/limit/sample | supported | `order_by`, `limit`, `offset`, `sample` | Ordered bounds are compiler-visible and batch-only; sampling requires explicit reproducibility policy and is batch-only. |
| Priority selection | supported | `select_first_qualified` | Declared business keys, eligibility, and priority order select one row per key; configured missing/tie failures report `REL-E0705`. |
| Distinct and deduplication | supported | `distinct`, `drop_duplicates` | Watermark form is streaming classified. |
| Grouping and standard aggregates | supported | `group_by`, `rollup`, `cube`, typed aggregates | Declared aggregate output schema. |
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

[API.ref.md](reference/API.ref.md).
