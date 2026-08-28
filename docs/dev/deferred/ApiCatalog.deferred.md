# API Catalog Deferred Work

This document records API work intentionally deferred after the current gate review. Current contract gates are in
[API Catalog Gates](../gated/ApiCatalog.gates.md); this document records postponed direction and adoption scope.

## Deferred Direction

The following items remain future work or caller-owned guidance:

## Non-Streaming Gates

### XML Helpers

XML remains low priority. A future `from_xml(value, schema=..., options=...)` / `to_xml(...)` contract must carry an
output Schema, normalized options, nullability, malformed-record policy, capability checks, and target evidence. XML
source reading remains caller-owned storage.

### Variant Mutation Profiles

Variant append, insert, set, and delete helpers remain reserved for a released target profile. The active retained-gate
work is indexed in [API Catalog Gates](../gated/ApiCatalog.gates.md); no mutation helper is implied by the released
Variant parsing and extraction slice.

### Geospatial Provider Boundary

The provider-neutral contract must define `geometry(srid=..., nullable=True)`, WKT construction and serialization,
SRID compatibility, and nullable `intersects`, `contains`, and `within` predicates without exposing provider imports.
Optional providers own materialization, lowering, imports, and runtime availability. Sedona is not bundled, and
`GEOGRAPHY`, CRS transformation, measurements, spatial joins, indexes, collections, non-WKT forms, and raw `ST_*`
wrappers remain outside the contract. The pinned Docker lane now supplies positive Sedona WKT round-trip evidence on
PySpark 3.5/4.0 and focused Spark Connect 3.5/4.0, but that evidence does not promote the broader provider surface.

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

- XML, provider-neutral geometry, Variant mutation, join-reordering, and directional as-of tie contracts;
- sampling refinements that preserve explicit reproducibility and batch-only streaming behavior; and
- missing-column union defaults plus nested, alias-preserving, and streaming schema-evolution rules.

## Admission Bar

- keep the API catalog, capability inventories, diagnostics, references, examples, and gap register synchronized;
- require symbolic execution and IR ownership before admitting a new transformation;
- require online/generated parity and generated-source evidence where both execution modes apply;
- require Spark Connect and streaming classifications to be explicit rather than inferred; and
- keep raw SQL, arbitrary callbacks, UDTFs, actions, storage, and lifecycle APIs at explicit caller-owned boundaries.

## V10 Adoption

The adopted core API slices are governed by the grouped plan
`docs/dev/planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md`. It covers provider-neutral Geometry,
sampling refinements, and missing-column union defaults with nested-struct and alias-preserving rules. XML, unreleased
Variant mutation profiles, and join reordering remain explicit catalog dispositions rather than automatic support
claims.
