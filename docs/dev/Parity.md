# Parity

This is the main PySpark parity and boundary register. It tracks detailed family-level coverage, postponed design
items, and deliberately unsupported API surface. It is a developer register, not a promise that Structure will become
a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

See the user-facing summary in [API.md](../API.md) and the unified API status tables in
[APICatalog.md](../APICatalog.md).

Current cross-family design gates are registered in [API Catalog gates](gated/ApiCatalog.gates.md), and
function-specific design gates are indexed in [Function Gates](gated/Functions.gates.md).

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
gaps. PySpark 4.1 adoption has a separate ledger in [APICatalog.md](../APICatalog.md) and
[V11.md](project-management/V11.md); it must not silently change the default `>=3.5,<4.1` baseline.

## Current Baseline

This register is current as of 2026-08-31. The default target remains PySpark `>=3.5,<4.1`, ordinary PySpark, with
Spark Connect claims only for completed compiler-visible batch features. The authoritative inventory is the intersection
of the PySpark 3.5.x and 4.0.x public APIs, not the newest Spark documentation.

Full SQL-function coverage means that every baseline function has exactly one disposition: implemented with evidence,
planned for a typed Structure contract, design-gated, caller-owned-guided, streaming-ineligible, or unsupported. It does
not mean that every function must be exposed under the same spelling or that arbitrary SQL strings become acceptable.

The eight examples raised during the audit resolve as follows: `hour` and `exp` were already implemented; the PySpark
spelling is `add_months` rather than `add_month`; and the implementation slices now cover `add_months`, `next_day`,
`acos`, `hypot`, `lpad`, `rpad`, `asin`, `atan`, `atan2`, `cos`, `degrees`, `ln`, `log10`, `radians`, `sin`, and
`tan`. `rand` is admitted as an explicitly nondeterministic scalar with a seed/reproducibility policy; its streaming
status remains target-evidence driven. The current String slice also covers `btrim`, function-form `contains`,
`find_in_set`, `format_number`, `position`, and `split_part`. The current collection slice also covers `cardinality`,
`array_size`, `array_max`, `array_min`, `array_join`, `arrays_overlap`, `get`, `sort_array`, and typed `concat`; `element_at` remains
one-based while `get` is zero-based. The implementation sequence is the [PySpark SQL
function coverage ExecPlan](planning/P08222601.PySpark-SQL-function-coverage.plan.md).

## Column Method Register

The symbolic `Expression` API is Structure's compiler-visible Column surface. It preserves the established Pythonic
spellings for existing methods, such as `is_null()`, `is_not_null()`, `null_safe_eq(...)`, and `get_field(...)`, while
matching PySpark method semantics. The first explicitly reconciled method slice is `substr(startPos, length)`.

| Disposition | Column methods | Structure surface or boundary |
| --- | --- | --- |
| Implemented | `between`, bitwise methods, `cast`, `contains`, `desc`/`asc` and null-order variants, `endswith`, `ilike`, `isin`, `like`, `rlike`, `startswith`, `substr` | Typed `Expression` methods; `__getitem__`, `get_field`, `with_field`, and `drop_fields` cover the corresponding nested access/mutation forms. |
| Function-form | `trim`, `lower`, and other SQL functions | Remain explicit `functions.*`-style Structure helpers; they are not PySpark `Column` methods in the active baseline. |
| Unsupported or design-gated | `alias`/`name`, `isNaN`, `when`/`otherwise`, `over`, `outer`, `transform` | Require a separate typed output, conditional, window, correlated-expression, or higher-order contract. |

`substr(startPos, length)` accepts integral literals or symbolic integral expressions, returns String, and propagates
nullability from the receiver and both bounds. Generated code uses canonical function-form `F.substr(...)`; this is
semantically equivalent to the method-form source expression.

## Docker Live Evidence Checkpoint

The repository Compose stack under `infra/compose/` was rerun on 2026-08-27. These results are runtime evidence for the
listed slices only; a passing infrastructure lane does not promote an unrelated family or clear a design gate.

| Backend | Collected | Passed | Skipped | Failed | Evidence boundary |
| --- | ---: | ---: | ---: | ---: | --- |
| `pyspark35` | 65 | 53 | 6 | 6 | Full integration/concept selection; foreachBatch restart, stream/static restart, stateless streaming, and Sedona geometry passed. |
| `pyspark40` | 65 | 56 | 3 | 6 | Full integration/concept selection; the same admitted ordinary-runtime slices passed. |
| `spark-connect35` | 24 | 15 | 9 | 0 | Focused Connect boundary/UDF, v7 generator/parsing, v9 geometry, and selected concept parity tests. Search and classic-only restart/state tests were excluded or skipped. |
| `spark-connect40` | 24 | 18 | 6 | 0 | The same focused Connect slice on the 4.0 target. |

The six ordinary failures are shared generated-result contract failures in four Search cases, the generated security
fixture, and the generated chained event-time window. They raise `TypeError: Generated transform executor must return a
stage-aware TransformResult when composed stage outputs are enabled`; therefore they are recorded as current open
implementation evidence, not as support. Exact Search vector retrieval, Search generated/online comparison, full Connect
Search proving, and broader streaming state/lifecycle claims remain gated or deferred.

## SQL Function Family Register

The table records the current family-level gaps. “Covered” includes a typed equivalent where the Structure API is
intentionally more explicit; “open” names the remaining PySpark functions or the contract decision still required.

| Family | Status | Covered now | Open gaps / boundary |
| --- | --- | --- | --- |
| Normal, conditional, predicate, and sort | partial | `literal`, `when`, null-control helpers, `isnull`, `isnotnull`, `isnan` | `equal_null`, function-form `like`/`ilike`/`regexp`/`regexp_like`/`rlike`, and null-ordering sort helpers. `expr` and `call_function` remain unsupported. |
| String | partial | `ascii`, `btrim`, `char`, `char_length`, `contains`, `elt`, `find_in_set`, `format_number`, `format_string`, `printf`, `lower`, `upper`, trim variants, `lpad`, `rpad`, `left`, `right`, `substring`, `substr`, `substring_index`, `split`, regex extraction/count/instruction/substr variants, `regexp_extract_all`, `concat_ws`, `length`, `locate`, `mask`, `octet_length`, `overlay`, `position`, `repeat`, `replace`, `initcap`, `reverse`, `soundex`, `translate`, `instr`, `levenshtein`, `split_part` | `randstr` and UTF-8 helpers. Regex patterns and capture-group indexes use the current literal-argument policy. |
| Numeric and mathematical | implemented | `abs`, `acos`, `acosh`, `asin`, `asinh`, `atan`, `atan2`, `atanh`, `bin`, `bround`, `cbrt`, `ceil`, `conv`, `cos`, `cosh`, `cot`, `csc`, `degrees`, `e`, `exp`, `expm1`, `factorial`, `floor`, `greatest`, `hex`, `hypot`, `least`, `ln`, `log`, `log10`, `log1p`, `log2`, `pmod`, `pi`, `pow`, `radians`, `rint`, `round`, `sec`, `sign`, `signum`, `sin`, `sinh`, `sqrt`, `tan`, `tanh`, `unhex`, `width_bucket` | All currently reviewed baseline numeric functions have typed contracts; `width_bucket` uses a positive bucket-count literal. |
| Random and seeded | partial | `rand`, `randn` with explicit seed/reproducibility policy | `uniform`, `randstr`, and other random helpers need separate contracts; streaming support is target-evidence driven. |
| Date and timestamp | partial | `add_months`, `date_add`, `date_sub`, `date_format`, `datediff`, `date_trunc`, `trunc`, calendar extraction including `dayofweek`, `dayofyear`, `hour`, `next_day`, `quarter`, `weekofyear`, `last_day`, and date/timestamp parsing | timezone/current-time functions, date part/name helpers, Unix/UTC conversion, `make_*` constructors, `months_between`, timestamp arithmetic/construction, `try_*` temporal helpers, and `weekday`. |
| Bitwise and binary | partial | typed Column bitwise methods, SQL `bit_count`, `bit_get`, `getbit`, `base64`, `unbase64`, `encode`, `decode`, `hex`, `unhex` | SQL shifts, `to_binary`, `try_to_binary`, and UTF-8/binary validation helpers. |
| Hash | implemented | `hash`, `xxhash64`, `crc32`, `md5`, `sha1`, `sha2` | Hashes remain non-identity and non-password-storage primitives; CRC-32 is a checksum rather than a cryptographic digest. |
| JSON and CSV | partial | Schema-carrying `from_json`, `to_json`, `from_csv`, `to_csv`, typed `get_json_object`, `json_array_length`, `json_object_keys`, and literal `schema_of_json`/`schema_of_csv` | `json_tuple` remains deferred until multi-column output schemas are supported; dynamic schema inference remains unsupported. |
| Arrays and higher-order functions | implemented | Typed array construction, lookup, mutation, set, concatenation, size, join, extrema, overlap, sort, shuffle, `sequence`, `slice`, `reduce`, `arrays_zip`, and symbolic callbacks through `arr_*`/`array_*` | Remaining baseline callback/array aliases require only a parity decision; stable `array_N` field names keep `arrays_zip` schema-visible. |
| Struct and map | partial | Typed map construction/lookup/entries/callbacks; schema constructors own struct shape | `create_map`, `map_from_arrays`, `str_to_map`, `named_struct`, and exact constructor parity. |
| Aggregates | partial | Core, boolean, statistical, `count_if`, `median`, population/sample standard-deviation and variance aliases, percentile, collection, and deterministic `mode` aggregates | `any_value`, `array_agg`, bitwise aggregates, `first`/`last`, `max_by`/`min_by`, `product`, regression aggregates, distinct/string aggregation, and sketch/bitmap aggregates. |
| Windows | partial | Typed ranking, lag/lead, value selection, and aggregate-window helpers | Raw `Column.over`, complete null-ordering options, and any aggregate/window form not admitted through typed `WindowSpec`. |
| Generators and partition transforms | partial | Typed array/map/struct generators and Variant TVFs | `stack`, generic PySpark generator spellings, and partition transforms `years`, `months`, `days`, `hours`, `bucket`; cardinality, schema, and streaming contracts are required. |
| Variant | partial | Released-profile parsing, extraction, validation, schema inspection, conversion, and TVF expansion | `is_valid_variant` is profile-gated; Variant mutations remain design-gated until a released target and mutation contract exist. |
| XML, URL, provider/runtime | design-gated or unsupported | No general XML or URL symbolic surface | XML and XPath functions, URL functions, geometry-provider functions, runtime metadata, reflection, encryption, and sketch/bitmap runtime integrations need separate contracts or remain caller-owned. |
| Python UDF/UDTF/custom types | implemented only for scalar UDFs | Opt-in scalar `@special(type="udf")` for ordinary batch | Pandas UDFs, UDTFs, UDTs, arbitrary callbacks, and implicit UDF conversion remain caller-owned. |

Updates to this register must be reflected in [APICatalog.md](../APICatalog.md), the machine-readable coverage
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
- API catalog rows in [APICatalog.md](../APICatalog.md).
