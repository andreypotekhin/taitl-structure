# API Tracker

This tracker records PySpark family-level coverage, postponed design items, and deliberately unsupported API surface.
It is a compatibility ledger, not a promise that Structure will become a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

See the API gateway in the [API documentation map](../API.md#api-documentation-map), family tables in
[APICompatibility.md](APICompatibility.md), and the concise public overview in [API.md](../API.md).

Current cross-family design gates are registered in [API Catalog gates](../dev/gated/ApiCatalog.gates.md), and
function-specific design gates are indexed in [Function Gates](../dev/gated/Functions.gates.md). Family summaries and
function-level contracts are maintained in [APICompatibility.md](APICompatibility.md) and its linked ledgers.

## Status

- `implemented`: shipped with the required capability, diagnostic, documentation, and verification evidence for the
  claimed target profile.
- `partial`: a family has useful implemented coverage, but one or more functions in the family remain open.
- `planned`: accepted implementation direction; it needs implementation, diagnostics, tests, or docs.
- `design-gated`: the API is a candidate, but its type, cardinality, determinism, streaming, or runtime contract must
  be designed before implementation.
- `caller-owned-guided`: Structure documents how to use native PySpark at the boundary, but does not compile the API.
- `streaming-ineligible`: the batch contract is or may be admissible, but Structure does not claim a streaming form.
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

The latest Spark docs may be useful for discovery, but features introduced after PySpark 4.0.x are not current-baseline
gaps. PySpark 4.1 adoption has a separate ledger below and in [V11.md](../dev/project-management/V11.md); it must not silently change the default `>=3.5,<4.1` baseline.

## Current Baseline

This register is current as of 2026-10-03. The default target remains PySpark `>=3.5,<4.1`, ordinary PySpark, with
Spark Connect claims only for completed compiler-visible batch features. The authoritative inventory is the intersection
of the PySpark 3.5.x and 4.0.x public APIs, not the newest Spark documentation.

Full SQL-function coverage means that every baseline function has exactly one disposition: implemented with evidence,
planned for a typed Structure contract, design-gated, caller-owned-guided, streaming-ineligible, or unsupported. It does
not mean that every function must be exposed under the same spelling or that arbitrary SQL strings become acceptable.

The official function-name census is reconciled against the pinned PySpark 3.5.6 and 4.0.0 indexes: 420 and 448
documented names, with 409 shared. The machine-readable
[source crosswalk](../../src/structure/plugin/pyspark/resources/pyspark-function-index-crosswalk.json) records those
snapshots, while the [transformation inventory](../../src/structure/plugin/pyspark/resources/pyspark-transformation-inventory.json)
classifies selected functions, explicit exclusions, target-only additions, and the `partitioning` namespace. This
closes name-scope accounting only; the [function-family ledgers](APICompatibility.md), runtime
evidence, and profile-specific semantic contracts remain authoritative for parity.

The eight examples raised during the audit resolve as follows: `hour` and `exp` were already implemented; the PySpark
spelling is `add_months` rather than `add_month`; and the implementation slices now cover `add_months`, `next_day`,
`acos`, `hypot`, `lpad`, `rpad`, `asin`, `atan`, `atan2`, `cos`, `degrees`, `ln`, `log10`, `radians`, `sin`, and
`tan`. `rand` is admitted as an explicitly nondeterministic scalar with a seed/reproducibility policy; its streaming
status remains target-evidence driven. The current String slice also covers `btrim`, function-form `contains`,
`find_in_set`, `format_number`, `position`, and `split_part`. The current collection slice also covers `cardinality`,
`array_size`, `array_max`, `array_min`, `array_join`, `arrays_overlap`, `get`, `sort_array`, and typed `concat`; `element_at` remains
one-based while `get` is zero-based. The implementation sequence is the [PySpark SQL
function coverage ExecPlan](../dev/planning/P08222601.PySpark-SQL-function-coverage.plan.md).

## PySpark 4.1 Adoption Tracker

These rows track the planned `>=4.1,<4.2` target profile; they do not widen the default `>=3.5,<4.1` baseline. The
primary target is ordinary PySpark. Spark Connect is admitted only with separate 4.1 runtime evidence. See the
[adoption design](../dev/design/V11PySpark41Adoption.design.md), [parity specification](../dev/specifications/V11PySpark41Parity.spec.md),
and [V11 project tracker](../dev/project-management/V11.md).

| PySpark 4.1 surface | Status | Structure contract or boundary | Admission evidence or caller remedy |
| --- | --- | --- | --- |
| `Column.transform` and higher-order additions | design-gated | Typed whole-expression callbacks declare result type and nullability; row-preserving. Array element mapping remains `arr_transform(...)`. | Type/nullability tests and ordinary online/generated parity; Connect evidence is a separate claim. |
| New deterministic scalar, string, binary, temporal, and collection functions | design-gated | Each admitted helper needs explicit type, nullability, generated spelling, and streaming behavior. | Reconcile the 4.0-to-4.1 inventory and add capability, tests, and evidence per function family. |
| Random and seeded helpers such as `random`, `uuid`, and `uniform` | design-gated | Must define seed, determinism, result type, and streaming policy; baseline `rand(...)` is tracked separately. | Keep newer helpers caller-owned until a versioned contract and target evidence are admitted. |
| `DataFrame.exists` and IN-subquery operations | design-gated | Correlated Boolean relation predicate with explicit aliases and null semantics. | Design and test duplicate, empty, null, and correlation cases; establish explain traceability and ordinary/Connect evidence. |
| `DataFrame.lateralJoin` | design-gated | Typed output schema, correlation scope, cardinality, and streaming classification. | Use caller-owned PySpark until the typed relation contract and evidence are complete. |
| Complex-valued `DataFrame.observe` metrics | design-gated | Observation is a metric side channel, not an implicit output-field mutation. | Keep caller-owned until metric typing, retrieval, and parity are specified. |
| KLL and Theta approximate sketches | design-gated | Opaque branded Binary types under the 4.1 profile; consumers require separate admission. | Use caller-owned PySpark until merge, dependency, determinism, and consumer semantics are specified and evidenced. |
| Arrow-optimized Python UDF/UDTF APIs | caller-owned-guided | Worker Python and callback-defined UDTF cardinality remain outside the symbolic compiler contract. | Use explicit raw/caller-owned hooks; no generated UDF/UDTF claim. |
| Row-based `transformWithState` | design-gated | `transform_with_state(...)` is implemented for ordinary PySpark `>=4.1,<4.2`; live timer, parity, and checkpoint-restart evidence is pending. | Use caller-owned Structured Streaming state code until the profile's support gate passes. |
| Pandas `transformWithStateInPandas` | design-gated | `transform_with_state_in_pandas(...)` is implemented for ordinary PySpark 4.0 and 4.1; Pandas dependencies and live parity/restart evidence are pending. | Use caller-owned Structured Streaming state code until the profile's support gate passes. |
| Delta tables | implemented; release-gated | Typed caller-bound mutations, schema evolution, snapshot/CDF/history/detail reads, generated/identity/default metadata, restore, optimize, and vacuum. | Live ordinary PySpark 4.1.0 / Delta 4.1.0 evidence passes; wider V11 matrix and Spark Connect remain pending. See [Delta compatibility](DeltaTables.compat.md). |
| Declarative Pipelines, SQL Scripting, Python Data Sources, readers/writers, and catalog/session APIs | unsupported | These are not compiler-visible DataFrame transformations. | Use native PySpark/Spark orchestration around Structure. |

## Column Method Register

The symbolic `Expression` API is Structure's compiler-visible Column surface. It preserves the established Pythonic
spellings for existing methods, such as `is_null()`, `is_not_null()`, `null_safe_eq(...)`, and `get_field(...)`, while
matching PySpark method semantics. The first explicitly reconciled method slice is `substr(startPos, length)`.

| Disposition | Column methods | Structure surface or boundary |
| --- | --- | --- |
| Implemented | `between`, bitwise methods, `cast`, `contains`, `desc`/`asc` and null-order variants, `endswith`, `ilike`, `isNaN`, `isin`, `like`, `rlike`, `startswith`, `substr` | Typed `Expression` methods; `isNaN` is spelled `isnan()`, `isin` supports variadic values or one list, and the plain string predicates accept literal or String-expression operands. `__getitem__`, `get_field`, `with_field`, and `drop_fields` cover the corresponding nested access/mutation forms. |
| Function-form | `trim`, `lower`, and other SQL functions | Remain explicit `functions.*`-style Structure helpers; they are not PySpark `Column` methods in the active baseline. |
| Unsupported or design-gated | `alias`/`name`, `when`/`otherwise`, `over`, `outer`, `transform` | Require a separate typed output, conditional, window, correlated-expression, or higher-order contract. |

`substr(startPos, length)` accepts integral literals or symbolic integral expressions, returns String, and propagates
nullability from the receiver and both bounds. Generated code uses canonical method-form
`receiver.substr(start, length)`, which preserves PySpark's valid argument contract for integer bounds.

`isin(...)` accepts the PySpark variadic form and a single list form. `contains(...)`, `startswith(...)`, and
`endswith(...)` accept a string literal or a String expression operand; the result is nullable when either operand is
nullable. SQL function helpers such as `trim(...)` and `lower(...)` remain explicit function-form APIs.

The focused live parity test passed through the configured Docker Compose runtimes on 2026-09-03: PySpark 3.5
(`1 passed` in 51.43 seconds) and PySpark 4.0 (`1 passed` in 71.30 seconds). The scenario covers nullable and
non-nullable String inputs, literal and symbolic integral bounds, online execution, and generated execution.

## Docker Live Evidence Checkpoint

The repository Compose stack under `infra/compose/` ran the current 244-test selection on all four backend lanes on
2026-10-04. Classic PySpark 3.5 completed; classic 4.0 reached its one-hour timeout, and both Connect broad runs were
stopped without summaries because they made no progress in the bounded observation window. Focused per-family evidence
is separately recorded. Runtime evidence applies only to the listed slices and does not promote unrelated families or
clear design gates.

| Backend | Collected | Passed | Skipped | Failed | Evidence boundary |
| --- | ---: | ---: | ---: | ---: | --- |
| `pyspark35` | 244 | 231 | 7 | 6 | Refreshed full integration/concept selection. Search/vector and V10 streaming checks pass; failures are V1/V2 order-hook schema contracts and V11 generated-import tests. |
| `pyspark40` | 244; incomplete at 3,600s | — | — | — | Reached 74%, during `v2/advanced_order_analytics`; Search and V10 restart checks had passed. |
| `spark-connect35` | 244; incomplete, stopped after >1h | — | — | — | Multiple failures observed in book-contract tests before Search; no final result. Focused Search: 22 passed/4 skipped; vector validation: 12 passed. |
| `spark-connect40` | 244; incomplete, stopped after about 27m | — | — | — | No progress beyond collection/initial test; focused Search and vector validation evidence is separate. |

The current classic 3.5 full selection no longer reproduces prior Search heap failures. The vector validation selection
passed on all four backends. Search focused proving cases pass on the four runtimes across the current and recent focused
runs. Broad full-selection result totals are unavailable on classic 4.0 and both Connect backends because those suites
timed out or were stopped before completion. SearchDocuments streaming and broader unsupported streaming shapes remain
gated.

### Focused P09302601 runtime evidence (2026-10-03)

These targeted lanes close the specific `btrim` and admitted-numeric checks tracked by the SQL baseline closeout. They
do not change the broader full-suite failures or promote unrelated functions.

| Contract | Runtime lanes | Test | Result |
| --- | --- | --- | --- |
| Row-valued trim set for `btrim` | Classic and Connect, PySpark 3.5.0 and 4.0.0 | [`test_string_functions_match_generated_execution_on_live_backend`](../../tests/integration/pyspark/v7/test_string_function_parity.py) | 1 passed / 231 deselected per lane; compares native `F.btrim` with generated Structure output and checks online/generated parity. |
| Admitted numeric functions | Classic and Connect, PySpark 3.5.0 and 4.0.0 | [`test_numeric_functions_match_native_and_generated_execution`](../../tests/integration/pyspark/v7/test_numeric_function_parity.py) | 1 passed / 231 deselected per lane; compares native, online, and generated results. |

## SQL Function Family Register

The table records the current family-level gaps. “Covered” includes a typed equivalent where the Structure API is
intentionally more explicit; “open” names the remaining PySpark functions or the contract decision still required.

| Family | Status | Covered now | Open gaps / boundary |
| --- | --- | --- | --- |
| Normal, conditional, predicate, and sort | partial | `literal`, `when`, null-control helpers, `isnull`, `isnotnull`, `isnan`, `equal_null`, function-form `like`/`ilike`/`regexp`/`regexp_like`/`rlike`; typed ascending/descending descriptors with null placement | Typed sort descriptors are implemented; `expr` and `call_function` remain unsupported because they erase the typed expression boundary. |
| String | partial | `ascii`, `btrim`, `char`, `char_length`, `contains`, `elt`, `find_in_set`, `format_number`, `format_string`, `printf`, `lower`, `upper`, trim variants, `lpad`, `rpad`, `left`, `right`, `substring`, `substr`, `substring_index`, `split`, regex extraction/count/instruction/substr variants, `regexp_extract_all`, `concat_ws`, `length`, `locate`, `mask`, `octet_length`, `overlay`, `position`, `repeat`, `replace`, `initcap`, `reverse`, `soundex`, `translate`, `instr`, `levenshtein`, `split_part` | UTF-8 validation and `randstr` are PySpark 4.0-only and target-gated. Regex patterns and capture-group indexes use the current literal-argument policy. |
| Numeric and mathematical | partial | `abs`, `acos`, `acosh`, `asin`, `asinh`, `atan`, `atan2`, `atanh`, `bin`, `bround`, `cbrt`, `ceil`, `ceiling`, `conv`, `cos`, `cosh`, `cot`, `csc`, `degrees`, `e`, `exp`, `expm1`, `factorial`, `floor`, `greatest`, `hex`, `hypot`, `least`, `ln`, `log`, `log10`, `log1p`, `log2`, `negate`, `negative`, `positive`, `power`, `pmod`, `pi`, `pow`, `radians`, `rint`, `round`, `sec`, `sign`, `signum`, `sin`, `sinh`, `sqrt`, `tan`, `tanh`, `unhex`, `width_bucket` | Exact-name aliases are implemented; `try_add`, `try_divide`, `try_multiply`, `try_subtract`, `try_avg`, and `try_sum` remain design-gated pending overflow, ANSI, and aggregate contracts. |
| Random and seeded | partial | `rand`, `randn` with explicit seed/reproducibility policy | PySpark 4.0 `randstr` and `uniform` are target-gated; newer random helpers need separate contracts; streaming support is target-evidence driven. |
| Date and timestamp | partial | Typed date arithmetic, formatting, truncation, extraction, epoch-day/epoch-unit conversion, query clocks, timezone conversion, timestamp construction/parsing, and qualified intervals. | All 59 inventoried temporal/query-clock names have per-function contracts. The official 3.5.6/4.0.0 function-index census is reconciled; semantic parity remains function- and profile-specific. Calendar Schema/Row materialization retains documented target-specific limits. See the per-family ledgers in [APICompatibility.md](APICompatibility.md). |
| Bitwise and binary | partial | typed Column bitwise methods, SQL `bitwise_not`, `bit_count`, `bit_get`, `getbit`, `shiftleft`, `shiftright`, `shiftrightunsigned`, `base64`, `unbase64`, `encode`, `decode`, `to_binary`, `try_to_binary`, `hex`, `unhex`, and typed AES-GCM helpers | UTF-8 validation is PySpark 4.0-only and target-gated; non-GCM/key-management helpers remain caller-owned. |
| Hash | implemented | `hash`, `xxhash64`, `crc32`, `md5`, `sha`, `sha1`, `sha2` | PySpark `sha` maps to Structure `sha1`; hashes remain non-identity and non-password-storage primitives, and CRC-32 is a checksum rather than a cryptographic digest. |
| JSON and CSV | partial | Schema-carrying `from_json`, `to_json`, `from_csv`, `to_csv`, typed `get_json_object`, `json_array_length`, `json_object_keys`, declared-schema `json_tuple`, and literal `schema_of_json`/`schema_of_csv` | `json_tuple` is top-level and nullable-string-only; dynamic schema inference remains unsupported. |
| Arrays and higher-order functions | implemented | Typed array construction, lookup, mutation, set, concatenation, size, join, extrema, overlap, sort, shuffle, `sequence`, `slice`, `reduce`, `arrays_zip`, and symbolic callbacks through `arr_*`/`array_*` | The per-function register records exact alias coverage and narrower contracts, including numeric-only `sequence`, literal-only `array_remove` items, typed-key `array_sort`, and stable `array_N` fields for `arrays_zip`; use native PySpark for the cases each row marks caller-owned. |
| Struct and map | implemented | Typed `create_map`, `map_from_arrays`, `str_to_map`, `named_struct`, map lookup/entries/callbacks; schema constructors own declared struct shape | Map keys are non-null, `str_to_map` delimiters are non-empty literals, and `named_struct` field names are unique non-empty literals. |
| Aggregates | partial | Core, boolean/statistical aliases including `some`, `mean`, `std`, population covariance, `count_if`, `median`, population/sample standard-deviation and variance aliases, subtotal metadata (`grouping`, `grouping_id`), percentile and typed `histogram_numeric`, collection, deterministic `mode`, `any_value`, `array_agg`, bitwise, `first`/`last`, `max_by`/`min_by`, `product`, `sum_distinct`, regression aggregates, and opaque HLL/Bitmap aggregates including typed `hll_union_agg` | PySpark 4.0 string-aggregation names are target-gated; KLL/Theta are design-gated for V11 and require a 4.1 profile; Count-Min remains caller-owned. |
| Windows | partial | Per-function typed ranking/distribution, lag/lead, value selection, and aggregate-window forms | Raw `Column.over` remains unsupported; expression-valued `lag`/`lead` defaults and value-function `ignoreNulls` controls remain caller-owned. |
| Generators and partition transforms | partial | Typed array/map/struct generators, Variant TVFs, and `stack` | `stack` has a fixed Schema/cardinality contract; generic generators and writer partition transforms remain caller-owned. |
| Relation distribution | implemented | `coalesce(partitions=...)`, hash `repartition(count, *keys)` / `repartition(*keys)`, and `repartition_by_range(...)` | Hash distribution is streaming-compatible with an advisory; range repartitioning remains batch-only. Writer partition transforms remain caller-owned. |
| Variant | partial | Released-profile parsing, extraction, validation, schema inspection, conversion, and TVF expansion | `is_valid_variant` and Variant mutations remain target-gated until a released profile has complete evidence. |
| XML, URL, provider/runtime | partial or unsupported | Typed URL encode/strict decode; tolerant decode is 4.0-gated and no XML/provider runtime claim is made | XML and XPath, URL parsing, runtime metadata, reflection, non-GCM encryption, and untyped sketch integrations remain caller-owned. Geospatial adoption is target-gated in P10012602. |
| Python UDF/UDTF/custom types | implemented only for scalar UDFs | Opt-in scalar `@special(type="udf")` for ordinary batch | Pandas UDFs, UDTFs, UDTs, arbitrary callbacks, and implicit UDF conversion remain caller-owned. |

Updates to this register must be reflected in the [API documentation map](../API.md#api-documentation-map), the machine-readable coverage
ledgers
under `src/structure/plugin/pyspark/resources/`, the relevant API reference, and the owning ExecPlan. A family must not
be marked implemented merely because one example function has tests.

## Admission Checklist

Before moving a gap to implemented, add or update:

- public reference docs and examples;
- backend capability support or an explicit unsupported diagnostic;
- symbolic execution and IR tests;
- generated PySpark rendering tests;
- execution tests when the feature runs online;
- Spark Connect evidence when the feature is claimed for that variant;
- streaming compatibility classification when the feature can receive streaming inputs;
- API documentation map rows in [API.md](../API.md#api-documentation-map).
