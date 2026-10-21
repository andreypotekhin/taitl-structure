# API Catalog Deferred Work

This document records API work intentionally deferred after the current gate review. Current contract gates are in
[API Catalog Gates](../gated/ApiCatalog.gates.md); this document records postponed direction and adoption scope.

## Deferred Direction

The following items remain future work or caller-owned guidance:

## SQL Baseline Deferred Index

Function-level migration rows are maintained in [PySpark SQL Baseline Gaps](../gaps/PySpark-SQL-baseline.gaps.md). Use
this table for deferred SQL-family direction; the gap register remains the source for exact PySpark names and remedies.

| Scope | Status | Deferred direction |
| --- | --- | --- |
| Dynamic JSON schemas | `design-gated` | Define a schema before execution; declared-schema `json_tuple` is supported separately. |
| Sketch and bitmap aggregates | `design-gated` | Define state, accuracy, mergeability, and result contracts. |
| Generic generators and writer partition transforms | `caller-owned-guided` | `stack` and typed relation distribution are implemented; generic/result-layout forms remain caller-owned. |
| XML | `design-gated` | Define declared schemas, options, malformed input, and parse/serialize evidence. |
| URL parsing and safe decoding | `target-gated` | `try_url_decode` requires PySpark 4.0; URL parsing needs a separate typed result-shape design. |
| Geospatial APIs | `target-gated` | Explicit helper namespaces and native/Sedona type rules are specified in Geospatial.spec.md and implemented under P10192602; managed runtime parity remains open. |
| Runtime metadata and reflection | `caller-owned-guided` | Keep non-query-clock runtime reads at an explicit native boundary. |
| Variant mutation helpers | `target-gated` | Define released target profile and mutation semantics. |

## Non-Streaming Gates

### XML Helpers

XML remains low priority. A future `from_xml(value, schema=..., options=...)` / `to_xml(...)` contract must carry an
output Schema, normalized options, nullability, malformed-record policy, capability checks, and target evidence. XML
source reading remains caller-owned storage.

### Variant Mutation Profiles

Variant append, insert, set, and delete helpers remain reserved for a released target profile. The active retained-gate
work is indexed in [API Catalog Gates](../gated/ApiCatalog.gates.md); no mutation helper is implied by the released
Variant parsing and extraction slice.

### Geospatial Namespace and Runtime Evidence

The active contract is in [Geospatial design](../design/Geospatial.design.md) and
[Geospatial.spec.md](../specifications/Geospatial.spec.md). Helper namespace selects the implementation; field types
select native or Sedona representation. There is no `geo_provider` scope. Sedona replaces Spark's unqualified
`st_*` SQL registrations, so same-session Apache/Sedona calls and inline Binary bridges remain gated. Managed
Databricks and Databricks Connect parity remain open in
[P10192602](../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md).

### Join Reordering

Any future `join_order("optimizer")` mode must be opt-in, rule-based, and explainable. Uncertainty must fall back to
source order.

The implemented nearest as-of contract rejects equidistant matches with `ties="error"`. Directional tie preferences
remain deferred until direction, tolerance, null-time, exact-match, and generated-lowering rules are explicit.

### Sampling

Sampling is a relation-level batch operation. It validates literal fraction and reproducibility policy, makes no
row-count or order guarantee, and remains a batch-materialization boundary for streaming.

### Missing-Column Set Composition

Missing-column union fills only nullable top-level fields with typed nulls in batch. Non-nullable fields require
explicit future defaults; nested structs, arrays, maps, aliases, and streaming support need separate contracts. The
streaming missing-column gate remains active in [Streaming Gates](../gated/Streaming.gates.md).

## Catalog Status Rule

Use `implemented` or `supported` when Structure owns the public contract, `unsupported` for an intentional boundary,
`design-gated` when a contract exists but evidence or implementation is incomplete, `streaming-ineligible` for batch
materialization boundaries, and `caller-owned-guided` for runnable caller integration.

## Deferred Scope

- XML, target-gated geospatial providers, Variant mutation, join-reordering, and directional as-of tie contracts;
- sampling refinements that preserve explicit reproducibility and batch-only streaming behavior; and
- missing-column union defaults plus nested, alias-preserving, and streaming schema-evolution rules.

## Admission Bar

- keep the API catalog, capability inventories, diagnostics, references, examples, and gap register synchronized;
- require symbolic execution and IR ownership before admitting a new transformation;
- require online/generated parity and generated-source evidence where both execution modes apply;
- require Spark Connect and streaming classifications to be explicit rather than inferred; and
- keep raw SQL, arbitrary callbacks, UDTFs, actions, storage, and lifecycle APIs at explicit caller-owned boundaries.

## V10 Adoption

The adopted core API slices and remaining gates are summarized in the [V10 project record](../project/V10.md),
[release evidence](../project/V10ReleaseEvidence.md), and current [API Catalog Gates](../gated/ApiCatalog.gates.md).
Geospatial provider adoption is specified in the [Geospatial design](../design/Geospatial.design.md). XML, unreleased
Variant mutation profiles, and join reordering remain explicit catalog dispositions rather than automatic support claims.
