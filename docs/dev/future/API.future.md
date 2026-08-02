# API future

## Deferred Work - ApiCatalogDesignGates.md
Include Deferred Work from ApiCatalogDesignGates.md

Ref: [ApiCatalogDesignGates.md](/docs/dev/design/ApiCatalogDesignGates.md)
Quote:
````
## Non-Streaming Gates

### XML Helpers

XML remains low priority. A future `from_xml(value, schema=..., options=...)` / `to_xml(...)` contract must carry an
output Schema, normalized options, nullability, malformed-record policy, capability checks, and target evidence. XML
source reading remains caller-owned storage.

### Variant And Geospatial Helpers

Variant support requires a public type before helper APIs. Geospatial support is an optional provider slice, never a raw
`ST_*` wrapper. The provider-neutral contract must define geometry, SRID compatibility, WKT construction and
serialization, and nullable spatial predicates without exposing provider imports.

### Join Reordering

Any future `join_order("optimizer")` mode must be opt-in, rule-based, and explainable. Uncertainty must fall back to
source order.

### Sampling

Sampling is a relation-level batch operation. It validates literal fraction and reproducibility policy, makes no row-count
or order guarantee, and remains a batch-materialization boundary for streaming.

### Missing-Column Set Composition

Missing-column union fills only nullable top-level fields with typed nulls in batch. Non-nullable fields require explicit
future defaults; nested structs, arrays, maps, aliases, and streaming support need separate contracts.

## Catalog Status Rule

Use `implemented` or `supported` when Structure owns the public contract, `unsupported` for an intentional boundary,
`design-gated` when a contract exists but evidence or implementation is incomplete, `streaming-ineligible` for batch
materialization boundaries, and `caller-owned-guided` for runnable caller integration.
````

To adopt from above:
- future Variant profile work for Spark 4.3+ mutation helpers; the released 4.0/4.2 literal, equality, and typed row-expansion slice is covered by V9;
- provider-neutral geometry contract with explicit SRID compatibility and no bundled provider dependency;
- sampling refinements that preserve explicit reproducibility and batch-only streaming behavior; and
- missing-column union defaults plus nested, alias-preserving, and streaming schema-evolution rules.

## Deferred Work - Gaps.md
Include Deferred Work from Gaps.md

Ref: [Gaps.md](/docs/dev/Gaps.md)
Quote:
````
API Gaps

This page tracks PySpark parity gaps, postponed design items, and deliberately unsupported API surface. It is a
developer backlog aid, not a promise that Structure will become a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

## Admission Checklist

Before moving a gap to implemented, add or update:

- public reference docs and examples;
- backend capability support or an explicit unsupported diagnostic;
- symbolic execution and IR tests;
- generated PySpark rendering tests;
- execution tests when the feature runs online;
- Spark Connect evidence when the feature is claimed for that variant;
- streaming compatibility classification when the feature can receive streaming inputs; and
- API catalog rows in `APICatalog.md`.
````

To adopt from above:
- keep the API catalog, capability inventories, diagnostics, references, examples, and gap register synchronized;
- require symbolic execution and IR ownership before admitting a new transformation;
- require online/generated parity and generated-source evidence where both execution modes apply;
- require Spark Connect and streaming classifications to be explicit rather than inferred; and
- keep raw SQL, arbitrary callbacks, UDTFs, actions, storage, and lifecycle APIs at explicit caller-owned boundaries.
