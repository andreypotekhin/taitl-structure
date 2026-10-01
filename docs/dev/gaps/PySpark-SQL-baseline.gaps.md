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

## Implementation Candidates

The names below are the current intake from the SQL Functions catalog. Milestone 1 of
[P09302601](../planning/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md) must replace grouped entries with one row
per verified PySpark function and record the 3.5/4.0 presence explicitly.

| Scope | Status | Structure work | Migration requirement |
| --- | --- | --- | --- |
| `randstr`, UTF-8 | candidate | Verify String/Binary semantics. | Preserve spelling or equivalent. |
| Time-zone, date-part | candidate | Add typed calls and nullability. | Match units, zones, timestamp types. |
| Unix timestamp, UTC | candidate | Add typed conversion helpers. | Match units, parsing, null behavior. |
| `make_*`, safe temporal | candidate | Add constructors and `try_*`. | Match validation and nullable failure. |
| `to_binary`, `try_to_binary` | candidate | Add format and failure contracts. | Match formats and malformed input. |
| String aggregation | candidate | Add typed `string_agg`/`listagg`. | Match delimiter, null, order, empty input. |
| `stack`, typed generators | candidate | Require schema and cardinality. | Match row multiplication, aliases, fields. |

## Design-Gated or Boundary Items

These entries remain visible for migration planning, but are not implementation promises. The owner documents the
missing contract or native-PySpark remedy before the status can change.

| Scope | Status | Missing contract or boundary | Migration remedy |
| --- | --- | --- | --- |
| `expr` / `call_function` | `unsupported` | Raw SQL removes typed ownership. | Use a native PySpark boundary. |
| Current-time | `design-gated` | Query-time nondeterminism, streaming. | Use native PySpark where required. |
| Crypto, encryption | `design-gated` | Key, IV, padding, provider, failure. | Keep security use caller-owned. |
| Dynamic JSON schema | `design-gated` | Output schema must be declared before execution. | Use declared-schema `json_tuple`, `from_json`, or native PySpark. |
| Sketch/bitmap aggregates | `design-gated` | State, accuracy, merge, result. | Native PySpark with evidence. |
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
