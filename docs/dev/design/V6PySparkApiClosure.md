# V6 PySpark API Closure

## Purpose

V6 closes the highest-value remaining gaps between Structure's typed PySpark DSL and the transformations used by its
own examples. The release does not wrap PySpark mechanically. It admits an API only when Structure can preserve its
schema, cardinality, determinism, target compatibility, diagnostics, traceability, online execution, and generated
execution.

The main outcome is less raw-hook code in the Security and Search examples. The supporting outcome is a smaller,
more maintainable PySpark plugin: focused components own one operation family instead of broad classes owning unrelated
symbolic capture, rendering, execution, and reporting behavior.

## Release Model

V6 has five delivery slices:

1. Create an API ledger, characterize current behavior, and extract focused plugin delegates without semantic change.
2. Add lambda-bound struct fields, analytic maximum, ordered collection, exactly-one validation, and formal implicit
   global aggregation semantics. Retire Security reconciliation hooks.
3. Add typed relation operations and retire Search hooks only where output-equivalence evidence proves the migration.
4. Deliver the separately specified bounded ordered `scan(...)` recurrence feature.
5. Complete live target evidence, documentation, executable specification coverage, and the Challenges C27--C34 audit.

The authoritative project-management decomposition is Sprint 23 through Sprint 27. The executable release plan is
`docs/dev/planning/done/P07242604.V6-pyspark-api-and-example-hook-retirement.plan.md`.

## Ownership Boundary

Core continues to own transform declarations, method order, lanes, outputs, lifecycle, and routing to one selected
plugin. The PySpark plugin owns every target meaning: symbolic expression and relation-operation capture, validation,
recipes, capabilities, online evaluation, generated source, traceability, explain, and target diagnostics.

A new API therefore travels through one complete path:

```text
authoring helper -> symbolic body -> immutable PySpark record -> recipe ->
capability/diagnostic -> online evaluator and generated renderer -> explain/traceability
```

No component may claim support by calling raw DataFrame code, executing a Spark action, or creating a hidden Python
fallback on only one path.

## Admission and Deferral

`docs/dev/Gaps.md` is the durable register for deferred and postponed work. The v6 ledger and coverage catalog provide
the detailed API/evidence view. Every status change updates all three in the same pull request:

- `docs/dev/Gaps.md` records the reason, user-facing escape hatch, and owning plan.
- `src/structure/plugin/pyspark/resources/pyspark-transformation-coverage.json` records the supported-target
  classification.
- `docs/APICatalog.md` records the release priority, contracts, examples, and evidence.

An unsupported or deferred API must name whether the correct boundary is an ordinary step, an explicit scalar UDF,
`@raw`, or caller-owned PySpark. It must never become an implicit promise merely because Spark offers it.

## Global Aggregation Decision

Structure already permits a schema-returning step to use aggregate expressions without first calling `group_by(...)`.
That form is an implicit global aggregate: all input rows comprise one group. V6 preserves it. It does not require a
new `group_all()` call and must not break existing aggregate-only step methods.

V6 formalizes the missing edge behavior: an aggregate-only output produces one row for an empty input where Spark's
aggregates produce a valid value, while a required non-aggregate output expression is rejected because there is no
current input row. The exact type/nullability and empty-array behavior belong to
`docs/dev/specifications/ImplicitGlobalAggregation.md`.

## Explicit Scalar UDF Decision

Scalar `@special(type="udf")` is already a supported opt-in PySpark feature. A developer explicitly declares its
return type and nullability; generated and online execution use the user-authored UDF body. It is opaque to the Spark
optimizer and therefore follows the existing `warn_on_udfs` policy. It is excluded from Spark Connect.

This is not an exception to the no-hidden-fallback rule. Structure must never convert unsupported Python source or a
missing typed API into a UDF automatically. Python UDTFs, Pandas UDFs, RDD operations, and driver materialization stay
outside the v6 DSL. V6 adds a small shipped example and documentation so users can deliberately choose among a
symbolic helper, an explicit scalar UDF, and a raw hook.

## Relation Operations

Relation operations change row identity, number, order, or source relation. They require more than scalar type
inference. Their shared design is in `docs/dev/design/TypedRelationOperations.md` and their implementation contract is
in `docs/dev/specifications/TypedRelationOperations.md`.

The first set is deliberately limited to typed generators, branchable exact-schema union composition, self aliases,
relation ordering, literal limits/offsets, relation assertions, bounded parent-hierarchy closure, and declared-key
first-qualified priority selection. These latter three remove the new Search cohort, relevance-context, and reranking
hooks without adopting general recursion or opaque surrogate identities. Sampling, physical-plan directives, dynamic
schemas, and source/sink ownership stay deferred.

The P1 design support lives in `docs/dev/design/TypedRelationOperations.md`. That document is the rationale for why
each P1 capability is admitted, which Search hook it supports, and which broader PySpark behavior remains outside the
release. The matching implementation contract is `docs/dev/specifications/TypedRelationOperations.md`, and the release
evidence ledger is `docs/APICatalog.md`.

## Plugin Decomposition

The following existing large modules become public-façade or orchestration shells over focused internal delegates:

- `dsl/operations_api.py`: grouping/aggregate, window/selection, collection, streaming, and relation builders.
- `dsl/expressions.py` and `dsl/InputScope.py`: expression/type construction, lambda/nested access, traversal, and
  relation recording.
- symbolic result construction, expression evaluation, online step running, step rendering, module rendering, and
  compiler traceability: one delegate per operation family, with top-level assembly only.

Extraction precedes feature work and retains public imports, recipes, generated source, and online behavior. The
existing endpoint-only PySpark app boundary remains mandatory.

## Non-Goals

- A one-to-one PySpark wrapper.
- Raw `Column`, `DataFrame`, `WindowSpec`, SQL strings, actions, source/sink lifecycle, and physical-plan APIs.
- Automatic UDF lowering, UDTFs, Pandas/RDD APIs, or driver-side fallback.
- Input-less, global, unbounded, persistent, or streaming recurrence; those remain outside the bounded `scan(...)`
  contract.
- Replacing the School matrix inversion hook, which intentionally performs a driver-side numerical algorithm.
