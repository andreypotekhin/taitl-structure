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

This register is current as of 2026-08-27. The default target remains PySpark `>=3.5,<4.1`, ordinary PySpark, with
Spark Connect claims only for completed compiler-visible batch features. The authoritative inventory is the intersection
of the PySpark 3.5.x and 4.0.x public APIs, not the newest Spark documentation.

Full SQL-function coverage means that every baseline function has exactly one disposition: implemented with evidence,
planned for a typed Structure contract, design-gated, caller-owned-guided, streaming-ineligible, or unsupported. It does
not mean that every function must be exposed under the same spelling or that arbitrary SQL strings become acceptable.

The eight examples raised during the audit resolve as follows: `hour` and `exp` were already implemented; the PySpark
spelling is `add_months` rather than `add_month`; and the implementation slices now cover `add_months`, `next_day`,
`acos`, `hypot`, `lpad`, `rpad`, `asin`, `atan`, `atan2`, `cos`, `degrees`, `ln`, `log10`, `radians`, `sin`, and
`tan`. `rand` is admitted as an explicitly nondeterministic scalar with a seed/reproducibility policy; its streaming
status remains target-evidence driven. The implementation sequence is the [PySpark SQL function coverage ExecPlan]
(planning/P08222601.PySpark-SQL-function-coverage.plan.md).

## SQL Function Family Register

The table records the current family-level gaps. “Covered” includes a typed equivalent where the Structure API is
intentionally more explicit; “open” names the remaining PySpark functions or the contract decision still required.

| Family | Status | Covered now | Open gaps / boundary |
| --- | --- | --- | --- |
| Normal, conditional, predicate, and sort | partial | `literal`, `when`, null-control helpers, `isnull`, `isnotnull`, `isnan` | `equal_null`, function-form `like`/`ilike`/`regexp`/`regexp_like`/`rlike`, and null-ordering sort helpers. `expr` and `call_function` remain unsupported. |
| String | partial | `ascii`, `char_length`, `lower`, `upper`, trim variants, `lpad`, `rpad`, `left`, `right`, `substring`, `substring_index`, `split`, regex extraction/replacement, `concat_ws`, `length`, `locate`, `octet_length`, `repeat`, `replace`, `initcap`, `reverse`, `translate`, `instr`, `levenshtein` | `btrim`, `char`, `contains`, `elt`, `find_in_set`, formatting, `mask`, `overlay`, `position`, `printf`, regex-count/instruction/substr variants, `sentences`, `soundex`, `split_part`, and UTF-8 helpers. |
| Numeric and mathematical | partial | `abs`, `acos`, `asin`, `atan`, `atan2`, `bround`, `ceil`, `cos`, `degrees`, `exp`, `floor`, `hypot`, `ln`, `log`, `log10`, `pow`, `radians`, `round`, `signum`, `sin`, `sqrt`, `tan` | `acosh`, `asinh`, `atanh`, `bin`, `cbrt`, `conv`, `cosh`, `cot`, `csc`, `e`, `expm1`, `factorial`, `greatest`, `hex`, `least`, `log1p`, `log2`, `pi`, `pmod`, `rint`, `sec`, `sign`, `sinh`, `tanh`, `unhex`, and `width_bucket`. |
| Random and seeded | partial | `rand` with explicit seed/reproducibility policy | `randn`, `uniform`, `randstr`, and other random helpers need separate contracts; streaming support is target-evidence driven. |
| Date and timestamp | partial | `add_months`, `date_add`, `date_sub`, `datediff`, `date_trunc`, `trunc`, calendar extraction including `hour`, `next_day`, and date/timestamp parsing | timezone/current-time functions, date formatting/parts, day/week/name helpers, Unix/UTC conversion, `last_day`, `make_*` constructors, `months_between`, quarter, timestamp arithmetic/construction, `try_*` temporal helpers, and week helpers. |
| Bitwise and binary | partial | typed Column bitwise methods, `base64`, `unbase64`, `encode`, `decode` | SQL bitwise functions, shifts, `to_binary`, `try_to_binary`, `hex`/`unhex`, and UTF-8/binary validation helpers. |
| Hash | partial | `hash`, `xxhash64`, `md5`, `sha1`, `sha2` | `crc32` and remaining baseline aliases need a parity decision; hashes remain non-identity and non-password-storage primitives. |
| JSON and CSV | partial | Schema-carrying `from_json`, `to_json`, `from_csv`, `to_csv` | `schema_of_csv`, `schema_of_json`, `get_json_object`, `json_array_length`, `json_object_keys`, and `json_tuple`. |
| Arrays and higher-order functions | partial | Typed array construction, lookup, mutation, set, sort, `sequence`, `slice`, and symbolic callbacks through `arr_*`/`array_*` | `cardinality`, `concat`, `array_join`, `array_max`, `array_min`, `array_size`, `arrays_overlap`, `arrays_zip`, `get`, `shuffle`, `sort_array`, and `reduce`; callback nullability and random shuffle need evidence. |
| Struct and map | partial | Typed map construction/lookup/entries/callbacks; schema constructors own struct shape | `create_map`, `map_from_arrays`, `str_to_map`, `named_struct`, and exact constructor parity. |
| Aggregates | partial | Core, boolean, statistical, percentile, collection, and deterministic `mode` aggregates | `any_value`, `array_agg`, bitwise aggregates, `count_if`, `first`/`last`, `max_by`/`min_by`, `median`, `product`, regression aggregates, population/sample aliases, distinct/string aggregation, and sketch/bitmap aggregates. |
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
