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
| Binary conversion | `implemented` | Typed `to_binary` and `try_to_binary` with literal formats. | Preserve format and failure behavior. |
| Unix-seconds formatting | `implemented` | Typed `from_unixtime` with numeric seconds and a literal format. | Preserve session-time-zone formatting. |
| Unix-seconds parsing | `implemented` | Typed `unix_timestamp` with String/Date/Timestamp inputs and the query-time default form. | Preserve the default format and query-time stability. |
| UTC conversion | `implemented` | Typed `to_utc_timestamp` and `from_utc_timestamp` with literal timezones. | Preserve timestamp nullability and timezone semantics. |

## Implementation Candidates

The names below are the current intake from the SQL Functions catalog. Milestone 1 of
[P09302601](../planning/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md) must replace grouped entries with one row
per verified PySpark function and record the 3.5/4.0 presence explicitly.

| Scope | Status | Structure work | Migration requirement |
| --- | --- | --- | --- |
| `randstr`, UTF-8 | candidate | Verify String/Binary semantics. | Preserve spelling or equivalent. |
| Time-zone conversion | candidate | Add `convert_timezone` with an explicit NTZ/source-zone contract. | Match source and target zone semantics. |
| Unix timestamp, UTC | candidate | Add remaining conversion helpers. | Match units, parsing, null behavior. |
| `make_*`, safe temporal | candidate | Add constructors and `try_*`. | Match validation and nullable failure. |
| UTF-8 validation | target-gated | PySpark 4.0-only validation helpers are outside the default intersection baseline. | Use native PySpark or add a versioned UTF-8 profile. |
| Current-time | implemented | Typed query-clock calls preserve six spellings and same-query equality metadata. | Use native PySpark for unmodeled timestamp variants. |
| AES-GCM | implemented | Typed GCM calls, symbolic keys, and nonce-risk warning. | Use native PySpark for excluded AES forms. |
| String aggregation | target-gated | PySpark 4.0-only `string_agg`/`listagg` are outside the 3.5/4.0 intersection baseline. | Use native PySpark on the 4.0 target or await a target-profile admission. |
| `stack`, typed generators | candidate | Require schema and cardinality. | Match row multiplication, aliases, fields. |

## Target-Line Additions Outside the Default Baseline

These APIs exist in one reviewed PySpark target line but not the 3.5/4.0 intersection. They are recorded here so the
baseline implementation count does not silently expand.

| PySpark API | Present in | Status | Migration remedy |
| --- | --- | --- | --- |
| `string_agg`, `listagg` | 4.0 | `target-gated` | Use native PySpark or add a versioned aggregate profile. |
| `is_valid_utf8`, `make_valid_utf8`, `try_validate_utf8`, `validate_utf8` | 4.0 | `target-gated` | Use native PySpark or add a versioned UTF-8 profile. |
| `randstr`, `uniform`, `uuid`, and related random helpers | 4.1 | `target-gated` | Track in the V11 adoption ledger with explicit seed semantics. |

## Design-Gated or Boundary Items

These entries remain visible for migration planning, but are not implementation promises. The owner documents the
missing contract or native-PySpark remedy before the status can change.

| Scope | Status | Missing contract or boundary | Migration remedy |
| --- | --- | --- | --- |
| `expr` / `call_function` | `unsupported` | Raw SQL removes typed ownership. | Use a native PySpark boundary. |
| Dynamic JSON | `caller-owned-guided` | Runtime inference cannot alter Schema. | Use declared parsing or native code. |
| Sketch/bitmap | `implemented` | Baseline HLL/Bitmap opaque types and consumers are implemented; KLL/Theta remain profile-gated and live evidence is pending. | Use native PySpark for unsupported profiles or Count-Min/observation metrics. |
| Generators, partitions | `design-gated` | Schema, aliases, cardinality, stream. | Typed or native PySpark. |
| Variant mutation | `target-gated` | Released profile and mutation contract. | Use the profile or native PySpark. |
| XML/URL/provider/runtime | `design-gated` | Provider ownership, typed results. | Caller-owned integrations. |
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
