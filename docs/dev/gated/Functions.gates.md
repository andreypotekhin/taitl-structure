# Function Gates

This document indexes function-specific design and evidence gates. The detailed PySpark family tables, parity status,
and caller-owned boundaries are maintained in [APITracker.md](../../compatibility/APITracker.md). Cross-family API decisions belong in
[API Catalog Gates](ApiCatalog.gates.md); streaming-specific decisions belong in [Streaming Gates](Streaming.gates.md).

## Gate Vocabulary

- `design-gated`: the function shape needs a typed contract or implementation evidence before support can be claimed.
- `target-gated`: support depends on a target or provider profile outside the default baseline.
- `streaming-ineligible`: the batch function may be supported, but no streaming form is claimed.
- `caller-owned-guided`: callers may use native PySpark at an explicit boundary; Structure does not compile the API.
- `deferred`: the function or family is intentionally postponed; its direction is recorded in the deferred register.

## Current Function Gates

### SQL baseline gate index

The row-level migration register is [PySpark SQL Baseline Gaps](../gaps/PySpark-SQL-baseline.gaps.md). This table keeps
the gate register searchable without duplicating the full PySpark inventory.

| Scope | Status | Missing contract or boundary |
| --- | --- | --- |
| Current-time functions | `implemented` | Typed query-clock metadata is implemented; ordinary target evidence remains a release check. |
| AES-GCM helpers | `implemented` | Typed GCM calls and explicit-IV warning are implemented; ordinary target evidence remains a release check. |
| Dynamic JSON schemas | `caller-owned-guided` | Runtime inference cannot alter a compiled Schema. |
| Sketch and bitmap aggregates | `implemented` | HLL/Bitmap are baseline features. KLL/Theta batch helpers are gated to exact PySpark 4.1 profiles; ordinary 4.1 also has watermarked streaming and same-configuration restart evidence. Connect streaming remains outside the backend contract; see the sketch ExecPlan. |
| `stack` | `implemented` | Fixed row multiplication and trailing-NULL padding use an explicit result Schema; classic and Connect batch parity is verified on PySpark 3.5 and 4.0. |
| Generic generators and writer partition transforms | `caller-owned-guided` | No compiler-visible schema/cardinality or output-layout contract. |
| Relation distribution | `implemented` | `coalesce(partitions=...)`, typed hash repartitioning, and batch-only range repartitioning are implemented. Writer partition transforms remain caller-owned; scalar `coalesce(...)` requires at least two values. |
| Variant mutation helpers | `target-gated` | Released target profile plus classic, Connect, generated/online, and streaming evidence. |
| URL encode/decode | `partial` | `url_encode` and strict `url_decode` are typed; `try_url_decode` is a 4.0 target gate. |
| XML helpers | `design-gated` | Declared schema, options, malformed-input behavior, and parse/serialize evidence. |
| Geospatial APIs | `target-gated` | Apache native 4.1 and Sedona 3.5/4.0/4.1 ordinary/Connect parity pass in isolated sessions. Managed Databricks parity remains open; Apache/Sedona work uses separate sessions with `PYSPARK-W2707` for mixed exact-4.1 transforms. See [P10192602](../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md). |
| Runtime metadata and reflection | `caller-owned-guided` | Query-clock expressions are the only admitted symbolic runtime reads. |
| `expr` / `call_function` | `unsupported` | Raw SQL removes typed expression ownership. |
| UDTFs, pandas UDFs, callbacks | `caller-owned-guided` | Arbitrary runtime behavior is outside typed transforms. |

### Family-level parity and evidence

The [APITracker.md](../../compatibility/APITracker.md) is authoritative for every reviewed PySpark function family. Open rows must retain
one precise disposition and identify the missing type, nullability, determinism, cardinality, target, streaming, or
runtime evidence. A family is not complete merely because one representative function has tests.

### Generators and partition transforms

Typed array, map, and struct generators are admitted only where schema and cardinality are explicit. `stack` has a
fixed Schema/cardinality design; generic generator spellings and writer partition transforms remain caller-owned.
Relation `coalesce(partitions=...)` is a row-preserving operation with a streaming throughput advisory. Scalar
`coalesce(...)` requires at least two expressions; a lone value should be used directly. Typed hash
`repartition(count, *keys)` / `repartition(*keys)` preserves rows and schema. A leading integer is always the count;
use an explicit literal expression for an integer key. Streaming hash repartitioning emits `STREAM-W0804`, while
range repartitioning remains batch-only.

### Variant mutations

Variant append, insert, set, and delete helpers are reserved for a released target profile. The active profile and
evidence gate is maintained in [API Catalog Gates](ApiCatalog.gates.md); classic, Connect, generated/online, and
streaming evidence are all required before release.

### Geospatial namespaces

The active contract is in [Geospatial.spec.md](../specifications/Geospatial.spec.md). Helper namespace selects the
implementation; field declaration selects native or Sedona representation. There is no provider setting or scope
precedence. Exact target and execution-mode evidence controls admission. Managed Databricks and Databricks Connect
remain open. Sedona's extension replaces Spark's unqualified `st_*` registrations, so execute Apache and Sedona
workloads in separate sessions. Exact-4.1 transforms containing both families emit `PYSPARK-W2707` with isolation and
WKB-plus-SRID guidance. The warning does not inspect ambient server registrations or separate transforms; see
[P10192602](../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md) .

### Random and order-sensitive functions

Seeded random functions, sampling, ordering, and selected-row helpers must state reproducibility, tie, null, and
streaming behavior. Their detailed family status remains in [APITracker.md](../../compatibility/APITracker.md), while postponed direction is in
[API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md).

## Admission Evidence

Before changing a function to `implemented` or `supported`, update the parity table, public API catalog, capability or
unsupported diagnostic, symbolic/IR tests, generated rendering, online execution, Spark Connect evidence where claimed,
and streaming classification. A skipped target lane is unavailable evidence, not a support result.

## Related Records

- [APITracker.md](../../compatibility/APITracker.md) is the detailed parity and boundary register.
- [API Catalog Gates](ApiCatalog.gates.md) owns cross-family API gates.
- [API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md) owns postponed API direction.
- [Streaming Gates](Streaming.gates.md) owns streaming-specific gates.
