# Function Gates

This document indexes function-specific design and evidence gates. The detailed PySpark family tables, parity status,
and caller-owned boundaries are maintained in the [Parity register](../../compatibility/APITracker.md). Cross-family API decisions belong in
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
| Sketch and bitmap aggregates | `implemented` | Baseline HLL/Bitmap opaque types and merge consumers are implemented; KLL/Theta remain profile-gated and evidence-bound. |
| `stack` | `implemented` | Fixed row multiplication and trailing-NULL padding use an explicit result Schema; classic and Connect batch parity is verified on PySpark 3.5 and 4.0. |
| Generic generators and writer partition transforms | `caller-owned-guided` | No compiler-visible schema/cardinality or output-layout contract. |
| Relation distribution | `implemented` | `coalesce(partitions=...)`, typed hash repartitioning, and batch-only range repartitioning are implemented. Writer partition transforms remain caller-owned; scalar `coalesce(...)` requires at least two values. |
| Variant mutation helpers | `target-gated` | Released target profile plus classic, Connect, generated/online, and streaming evidence. |
| URL encode/decode | `partial` | `url_encode` and strict `url_decode` are typed; `try_url_decode` is a 4.0 target gate. |
| XML helpers | `design-gated` | Declared schema, options, malformed-input behavior, and parse/serialize evidence. |
| Geospatial providers | `target-gated` | Native 4.1 root APIs and namespaced external providers are tracked in P10012602. |
| Runtime metadata and reflection | `caller-owned-guided` | Query-clock expressions are the only admitted symbolic runtime reads. |
| `expr` / `call_function` | `unsupported` | Raw SQL removes typed expression ownership. |
| UDTFs, pandas UDFs, callbacks | `caller-owned-guided` | Arbitrary runtime behavior is outside typed transforms. |

### Family-level parity and evidence

The [Parity register](../../compatibility/APITracker.md) is authoritative for every reviewed PySpark function family. Open rows must retain
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

### Geospatial providers

Native Geometry/Geography is a PySpark 4.1 target gate. External provider helpers are namespaced and require matching
provider scope. The default baseline makes no spatial support claim; see
[P10012602](../planning/P10012602.Geospatial-provider-boundaries.plan.md).

### Random and order-sensitive functions

Seeded random functions, sampling, ordering, and selected-row helpers must state reproducibility, tie, null, and
streaming behavior. Their detailed family status remains in [Parity](../../compatibility/APITracker.md), while postponed direction is in
[API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md).

## Admission Evidence

Before changing a function to `implemented` or `supported`, update the parity table, public API catalog, capability or
unsupported diagnostic, symbolic/IR tests, generated rendering, online execution, Spark Connect evidence where claimed,
and streaming classification. A skipped target lane is unavailable evidence, not a support result.

## Related Records

- [Parity](../../compatibility/APITracker.md) is the detailed parity and boundary register.
- [API Catalog Gates](ApiCatalog.gates.md) owns cross-family API gates.
- [API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md) owns postponed API direction.
- [Streaming Gates](Streaming.gates.md) owns streaming-specific gates.
