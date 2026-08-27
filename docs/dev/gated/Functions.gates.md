# Function Gates

This document indexes function-specific design and evidence gates. The detailed PySpark family tables, parity status,
and caller-owned boundaries are maintained in the [Parity register](../Parity.md). Cross-family API decisions belong in
[API Catalog Gates](ApiCatalog.gates.md); streaming-specific decisions belong in [Streaming Gates](Streaming.gates.md).

## Gate Vocabulary

- `design-gated`: the function shape needs a typed contract or implementation evidence before support can be claimed.
- `target-gated`: support depends on a target or provider profile outside the default baseline.
- `streaming-ineligible`: the batch function may be supported, but no streaming form is claimed.
- `caller-owned-guided`: callers may use native PySpark at an explicit boundary; Structure does not compile the API.
- `deferred`: the function or family is intentionally postponed; its direction is recorded in the deferred register.

## Current Function Gates

### Family-level parity and evidence

The [Parity register](../Parity.md) is authoritative for every reviewed PySpark function family. Open rows must retain
one precise disposition and identify the missing type, nullability, determinism, cardinality, target, streaming, or
runtime evidence. A family is not complete merely because one representative function has tests.

### Generators and partition transforms

Typed array, map, and struct generators are admitted only where schema and cardinality are explicit. `stack`, generic
PySpark generator spellings, and partition transforms remain open in the parity table until their cardinality, schema,
and streaming contracts are decided.

### Variant mutations

Variant append, insert, set, and delete helpers are reserved for a released target profile. The active profile and
evidence gate is maintained in [API Catalog Gates](ApiCatalog.gates.md); future-profile direction is in
[API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md).

### Random and order-sensitive functions

Seeded random functions, sampling, ordering, and selected-row helpers must state reproducibility, tie, null, and
streaming behavior. Their detailed family status remains in [Parity](../Parity.md), while postponed direction is in
[API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md).

## Admission Evidence

Before changing a function to `implemented` or `supported`, update the parity table, public API catalog, capability or
unsupported diagnostic, symbolic/IR tests, generated rendering, online execution, Spark Connect evidence where claimed,
and streaming classification. A skipped target lane is unavailable evidence, not a support result.

## Related Records

- [Parity](../Parity.md) is the detailed parity and boundary register.
- [API Catalog Gates](ApiCatalog.gates.md) owns cross-family API gates.
- [API Catalog Deferred Work](../deferred/ApiCatalog.deferred.md) owns postponed API direction.
- [Streaming Gates](Streaming.gates.md) owns streaming-specific gates.
