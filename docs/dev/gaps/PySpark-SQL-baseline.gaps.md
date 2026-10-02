# PySpark SQL Baseline Gaps

This is the function-level migration gap register for the ordinary PySpark `>=3.5,<4.1` baseline. The baseline is the
intersection of the public PySpark 3.5.x and 4.0.x APIs. It complements the family summary in
[Parity](../Parity.md) and the public status table in [APICatalog.md](../../APICatalog.md).

The register targets PySpark parity for migration: preserve the PySpark name and semantics where Structure can own a
typed contract; record an explicit Structure equivalent where the public spelling must differ; and give every remaining
function a precise gate or caller remedy. It is not a promise to expose arbitrary SQL strings or Python callbacks.

## Status Vocabulary

| Status | Meaning |
| --- | --- |
| `implemented` | Structure owns a typed equivalent with required evidence. |
| `candidate` | The baseline function fits the typed model and is ready for implementation after audit. |
| `design-gated` | A type, schema, cardinality, time, state, security, or runtime contract is missing. |
| `target-gated` | Support depends on a PySpark, provider, or runtime profile outside the baseline. |
| `caller-owned-guided` | Migration remains possible through native PySpark at an explicit boundary. |
| `unsupported` | The API conflicts with the compiler-visible Structure contract. |

## Contract Closure

| Scope | Status | Structure work | Migration requirement |
| --- | --- | --- | --- |
| Ordering descriptors | `implemented` | Harden validation across consumers. | Preserve direction and null placement. |
| `stack` | `implemented` | `stack(rows, *values, as_=Schema, scope=None)` fixes row multiplication, position-wise common types, and trailing-NULL padding before execution. | Preserve output aliases, types, nullability, and streaming row expansion. |
| Relation distribution | `partial` | `coalesce(partitions=...)` and typed hash `repartition(count, *keys)` / `repartition(*keys)` preserve row/schema; range repartition remains batch-only. | Preserve the leading-integer count rule and avoid ordering or stable-partition promises. |
| Binary conversion | `implemented` | Typed `to_binary` and `try_to_binary` with literal formats. | Preserve format and failure behavior. |
| Unix-seconds formatting | `implemented` | Typed `from_unixtime` with numeric seconds and a literal format. | Preserve session-time-zone formatting. |
| Unix-seconds parsing | `implemented` | Typed `unix_timestamp` with String/Date/Timestamp inputs and the query-time default form. | Preserve the default format and query-time stability. |
| UTC conversion | `implemented` | Typed `to_utc_timestamp` and `from_utc_timestamp` with literal timezones. | Preserve timestamp nullability and timezone semantics. |
| NTZ timezone conversion | `implemented` | `convert_timezone(source_tz, target_tz, timestamp_ntz)` accepts typed String zone expressions and PySpark's `None` source-zone default. | Keep wall-clock NTZ values distinct from instant-based Timestamp values. |
| Date construction | `implemented` | `make_date(year, month, day)` accepts typed Integer/Long expressions and returns nullable Date. | Invalid-component behavior follows Spark ANSI configuration. |
| NTZ parsing | `implemented` | `to_timestamp_ntz` parses a String expression with an optional typed String format expression to nullable TimestampNTZ. | Keep result type independent of `spark.sql.timestampType`; malformed text returns null. |

## Implementation Candidates

The names below are the current intake from the SQL Functions catalog. Milestone 1 of
[P09302601](../planning/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md) must replace grouped entries with one row
per verified PySpark function and record the 3.5/4.0 presence explicitly.

| Scope | Status | Structure work | Migration requirement |
| --- | --- | --- | --- |
| `randstr`, UTF-8 | candidate | Verify String/Binary semantics. | Preserve spelling or equivalent. |
| Time-zone conversion | implemented | Typed `convert_timezone` requires a `timestamp_ntz` value and typed String zone expressions; `source_tz=None` uses the session zone. | Preserve PySpark's TimestampNTZ result and source/target zone semantics. |
| Unix timestamp, UTC | candidate | Add remaining conversion helpers. | Match units, parsing, null behavior. |
| Timestamp constructors and safe temporal | candidate | Resolve configuration-sensitive NTZ/LTZ constructors and remaining admitted `try_*` conversions. | Match target type, validation, and nullable-failure behavior. |
| UTF-8 validation | target-gated | PySpark 4.0-only validation helpers are outside the default intersection baseline. | Use native PySpark or add a versioned UTF-8 profile. |
| Current-time | implemented | Typed query-clock calls preserve six spellings and same-query equality metadata. | Use native PySpark for unmodeled timestamp variants. |
| AES-GCM | implemented | Typed GCM calls, symbolic keys, and nonce-risk warning. | Use native PySpark for excluded AES forms. |
| String aggregation | target-gated | PySpark 4.0-only `string_agg`/`listagg` are outside the 3.5/4.0 intersection baseline. | Use native PySpark on the 4.0 target or await a target-profile admission. |

## Target-Line Additions Outside the Default Baseline

These APIs exist in one reviewed PySpark target line but not the 3.5/4.0 intersection. They are recorded here so the
baseline implementation count does not silently expand.

| PySpark API | Present in | Status | Migration remedy |
| --- | --- | --- | --- |
| `string_agg`, `listagg` | 4.0 | `target-gated` | Use native PySpark or add a versioned aggregate profile. |
| `is_valid_utf8`, `make_valid_utf8`, `try_validate_utf8`, `validate_utf8` | 4.0 | `target-gated` | Use native PySpark or add a versioned UTF-8 profile. |
| `randstr`, `uniform`, `uuid`, and related random helpers | 4.1 | `target-gated` | Track in the V11 adoption ledger with explicit seed semantics. |
| Native `st_geomfromwkb`, `st_geogfromwkb`, `st_asbinary`, `st_srid`, `st_setsrid` | 4.1 | `target-gated` | Track native Geometry/Geography in P10012602 with provider and mode evidence. |

## Design-Gated or Boundary Items

These entries remain visible for migration planning, but are not implementation promises. The owner documents the
missing contract or native-PySpark remedy before the status can change.

| Scope | Status | Missing contract or boundary | Migration remedy |
| --- | --- | --- | --- |
| `expr` / `call_function` | `unsupported` | Raw SQL removes typed ownership. | Use a native PySpark boundary. |
| Dynamic JSON | `caller-owned-guided` | Runtime inference cannot alter Schema. | Use declared parsing or native code. |
| Sketch/bitmap | `implemented` | Baseline HLL/Bitmap opaque types and consumers are implemented; KLL/Theta remain profile-gated and live evidence is pending. | Use native PySpark for unsupported profiles or Count-Min/observation metrics. |
| Generic generators | `caller-owned-guided` | Only `stack` has a fixed typed result contract; raw generators lack static schema and cardinality. | Use native PySpark at a declared result boundary. |
| Writer partition transforms | `caller-owned-guided` | Output file layout remains separate from relation distribution and caller-owned. | Use native PySpark writer partition transforms. |
| Variant mutation | `target-gated` | Released profile plus classic, Connect, generated/online, and streaming evidence. | Use the profile or native PySpark. |
| XML | `design-gated` | Declared schema, normalized options, malformed-record policy, and parse/serialize evidence. | Use a native PySpark boundary. |
| URL | `partial` | `url_encode` and strict `url_decode` are baseline row-local; `try_url_decode` is 4.0 target-gated. | Use native PySpark for safe decoding or unsupported profiles. |
| Geospatial providers | `target-gated` | Native root `st_*` is 4.1+; external providers are namespaced and scope-matched. | Use native PySpark or an explicit Binary boundary. |
| Runtime metadata and reflection | `caller-owned-guided` | Query-clock expressions are the only admitted symbolic runtime reads. | Use an explicit native PySpark boundary. |
| UDTFs, pandas UDFs, callbacks | `caller-owned-guided` | Arbitrary runtime behavior. | Use explicit native PySpark. |

## Reconciliation Rules

Every baseline function receives exactly one row in the completed register. Each row records PySpark 3.5.x and 4.0.x
presence, PySpark signature, Structure symbol or equivalent, status, evidence, and migration remedy. A name absent from
either target line is not a default-baseline implementation task; it belongs in the target adoption ledger.

Parity means behavior, not merely name parity. Reconciliation covers argument order and literal rules, result type,
nullability, determinism, row cardinality, error behavior, ordering, target profile, and streaming classification.
Structure may use a more explicit typed API, but the gap row must show how a PySpark user maps the operation.

## Ownership and Updates

The owning ExecPlan is [P09302601](../planning/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md). Update this table
with [Parity](../Parity.md), [Function Gates](../gated/Functions.gates.md),
[API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md), the public catalog, capability ledgers, API references,
and tests whenever a disposition changes.
