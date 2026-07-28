# V7 PySpark Transformation Coverage

## Purpose

v7 expands Structure's typed PySpark transformations without weakening its compiler-visible contract. A user can look up a PySpark 3.5.x/4.0.x transformation family, find its Structure spelling and support state, and learn whether it works for batch, caller-owned streaming, or neither. A feature is not supported merely because generated code could call it.

## Authoritative Catalog

The machine-readable companion catalog remains
`src/structure/plugin/pyspark/resources/pyspark-transformation-coverage.json`. Its inventory companion,
`pyspark-transformation-inventory.json`, enumerates every selected baseline family; the compatibility test requires an
exact one-to-one classification and verifies that every evidence path exists. Reusing this checked source prevents v7
from creating a second, drifting catalog.

At v7 kickoff, the catalog contains thirty-six baseline families: thirty-one supported, three deferred, and two
intentionally unsupported. v7 commits to admit the three deferred families: binary encoding, schema-carrying JSON/CSV
conversion, and deterministic mode. The unsupported families remain raw column aliases and raw `WindowSpec`; Structure
uses schema-owned names and typed window contracts.

## v7 Admission Record

Every v7 catalog entry must additionally identify its delivery slice, a dependency delegate, and a streaming state:

- **delivery slice** is `foundation`, `generator expansion`, `binary values`, `typed parsing`, `deterministic mode`,
  `stream-static enrichment`, `streaming composition`, or retained backlog;
- **dependency delegate** identifies the focused PySpark component that must be characterized before the feature is
  added;
- **streaming state** is `stateless`, `stateful with declared bound`, `batch-only`, or `not applicable`.

The catalog entry may become `supported` only when the public spelling, exact input/output schema, null/empty/duplicate
semantics, cardinality, capability decision, diagnostics, recipe, evaluator, renderer, explain/traceability record,
and online/generated parity tests exist. A classic-PySpark 3.5/4.0 live result is required before a release claim.
Spark Connect and streaming claims are independent of the ordinary batch claim.

## Delivery Order

1. **Foundation:** characterize and divide the oversized PySpark operation, expression, scope, result, evaluation,
   execution, rendering, and traceability responsibilities at the seams used by later work. Preserve public imports and
   existing generated output.
2. **Generator expansion:** specify `explode`, `explode_outer`, `posexplode`, `posexplode_outer`, `inline`, and
   `inline_outer` as separate typed relation operations. The existing `posexplode_struct(...)` remains the reference
   contract, not a generic shortcut.
3. **Binary values:** add the public Binary type and typed base64/charset conversion with cross-version invalid-input
   semantics.
4. **Typed parsing:** add exact Schema-carrying JSON and CSV conversion with normalized literal options and no schema
   inference.
5. **Deterministic mode:** add PySpark-named `mode(...)` after `group_by(...)`; use the native deterministic argument
   on PySpark 4.0 and an equivalent typed compatibility aggregate on PySpark 3.5.
6. **Streaming adoption:** deliver stream-static inner/left/left-semi enrichment, then design-gate one stateful
   operation followed only by stateless transformations. Every stage requires file-stream restart evidence.

## Generator Expansion Contract

The first feature slice covers row generators because they are high-value DataFrame transformations with explicit
schema and cardinality consequences. Each variant is a separate operation record and must declare:

- the source collection type and required element nullability;
- the exact generated fields, including ordinal and outer-row nullability;
- empty and null collection behavior;
- row cardinality (`zero-or-more`, `one-or-more`, or outer-preserving);
- whether the operator consumes or preserves current relation ordering;
- batch, Spark Connect, and streaming compatibility; and
- source, recipe, evaluator, renderer, traceability, diagnostic, and example evidence.

`inline` and `inline_outer` additionally require an array-of-struct element Schema and produce one declared field per
struct member. No generator may return an untyped list, a raw PySpark Column, or dynamic output names. Generator
expansion is batch-only at first; its streaming state is `batch-only` until the streaming gate establishes a separate
Spark-supported contract.

## Deferred-Family Admission

The concrete contracts for Binary encoding, schema-carrying JSON/CSV conversion, and deterministic mode are in
`docs/dev/design/V7DeferredPySparkFamilies.md`. Their catalog rows remain deferred until their stated target evidence
passes, but they are v7 committed delivery—not retained backlog.

## Exclusions

Readers, writers, actions, raw SQL, catalog management, RDD/Pandas APIs, arbitrary callbacks, untyped UDTFs, and
streaming lifecycle operations remain excluded. Callers continue to own sources, sinks, checkpoints, triggers, output
modes, and query lifecycle.
