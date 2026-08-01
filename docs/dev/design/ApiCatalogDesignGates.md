# API Catalog Design Gates

## Purpose

This design explains how APICatalog rows earn a real Structure contract. An open row must say whether it is
implemented, unsupported, streaming-ineligible, caller-owned-guided, or design-gated with a design/specification link.
The catalog is not a one-to-one PySpark wrapper: Structure admits only typed, compiler-visible, explainable, and
testable transformations.

## Coverage and Admission

The coverage catalog is the single source of truth for the PySpark 3.5.x/4.0.x transformation intersection. Each row
names the PySpark API family, Structure spelling, status, target variants, input/output type and nullability, cardinality
effect, semantic differences, exclusion reason, and evidence. A feature becomes supported only after source syntax, IR,
shared online/generated recipes, capability checks, actionable diagnostics, explain/traceability, tests, public docs,
and target evidence agree.

The delivery order is high-frequency scalar and conditional expressions, nested values and parsing, typed relational
and analytical operations, caller-owned streaming migration, cardinality-gated generators, and release closure. Raw
SQL, raw `WindowSpec`, dynamic schemas, arbitrary callbacks, source/sink lifecycle, and hidden UDF or RDD fallback stay
outside the contract.

## Non-Streaming Gates

### XML Helpers

XML remains low priority. A future `from_xml(value, schema=..., options=...)` / `to_xml(...)` contract must carry an
output Schema, normalized options, nullability, malformed-record policy, capability checks, and target evidence. XML
source reading remains caller-owned storage.

### Variant and Geospatial Helpers

Variant support requires a public type before helper APIs. The bundled PySpark plugin admits the implemented Variant
slice only for resolved PySpark 4 profiles; later-profile mutation and row-expansion helpers remain target-gated.
Geospatial support is an optional Apache Sedona 1.9.0 provider slice, never a bundled dependency or raw `ST_*` wrapper.
The provider-neutral contract is `geometry(srid=..., nullable=True)`, WKT construction/serialization, and nullable
`intersects`, `contains`, and `within` predicates. SRID is a positive literal in the type; unequal SRIDs are
incompatible. `GEOGRAPHY`, CRS transformation, measurements, spatial joins/indexes, collections, and non-WKT forms
remain outside the slice. Generated code uses the common Geometry SQL contract and must not expose provider imports.

### Join Reordering

There is no public `join_order(...)` helper in the current catalog. Any future optimizer mode must be opt-in, rule-based,
and explainable: only a contiguous chain of inner equality joins without hooks, assertions, dedupe, set operations,
ordering, limits, projections that read later scopes, or streaming joins may move. Explain must show source and chosen
orders, relation hints must follow their relation, and uncertainty must fall back to source order.

### Nearest As-Of Joins

Nearest as-of joins are selected-row joins with explicit direction, tolerance, null-time, exact-match, and tie rules.
The implemented first contract uses `ties="error"`, rejects null times, makes distance and ranking visible in generated
PySpark, and remains outside streaming. Directional tie preferences remain design-gated.

### Aggregate Aliases

Aggregate aliases are closed as unsupported/not applicable. Output Schema constructors and field aliases own aggregate
names; a second `GroupedData.agg` aliasing API would create ambiguity in generated code, traceability, and diagnostics.

### Sampling

Sampling is a relation-level batch operation. `sample(fraction, with_replacement=False, seed=None,
reproducible=True)` validates literal fraction and reproducibility policy, makes no row-count or order guarantee, and
renders the public DataFrame `sample(...)` call. Streaming sampling is a batch-materialization boundary.

### Missing-Column Set Composition

`union_by_name(..., allow_missing_columns=True)` fills only nullable top-level missing fields with typed nulls in batch.
Non-nullable fields require explicit future defaults; nested structs, arrays, maps, aliases, and streaming support need
separate contracts. The exact-schema union behavior remains unchanged.

## Streaming Gates

Streaming gate families cover chained event-time windows, chained stateful operators, selected-row helpers, analytic
window projections, side-effect APIs, and arbitrary state APIs. Their state, watermark, output-mode, caller-ownership,
diagnostic, generated-code, and live-evidence contracts are consolidated in the canonical
`docs/dev/design/SparkStreaming.md` and `docs/dev/specifications/SparkStreaming.md` sections.

## Catalog Status Rule

Use `implemented` or `supported` when Structure owns the public contract, `unsupported` for an intentional boundary,
`design-gated` when a contract exists but evidence or implementation is incomplete, `streaming-ineligible` for batch
materialization boundaries, and `caller-owned-guided` for runnable caller integration. Mixed rows must name supported and
gated portions precisely.
