# Typed Relation Operations

## Problem

Scalar expressions describe a value for the current row. Existing Structure operation records already distinguish
row-preserving projection, row-filtering predicates, aggregation, joins, and selected-row operations. Several Search
example hooks need a different kind of operation: they create rows, combine two complete relations, name two views of
the same relation, or establish a presentation order. Treating these as scalar helpers would lose their schema and
cardinality meaning; treating them as raw hooks hides useful plans from Structure.

## Design

A relation operation is an immutable PySpark-plugin operation record with these facts:

- source relation or relations, including occurrence identity for an alias;
- declared output schema or output schemas;
- cardinality category and duplicate semantics;
- ordering semantics, if any;
- null/empty behavior;
- batch, streaming, and Spark Connect classification;
- source provenance, capability name, and diagnostics.

The operation is captured inside the normal active `PySparkSymbolicContext`. Core retains it as opaque plugin body
data. PySpark validates it, maps it to a recipe, evaluates it online, renders the same public PySpark APIs, and adds a
traceability/explain fact.

## Cardinality Categories

The operation model uses the existing cardinality vocabulary where possible and adds no ambiguous “transformation”
bucket:

- **row-expanding**: one input row yields zero or more output rows (`posexplode`, `explode`, `inline`).
- **set-combining**: rows from two compatible relations are concatenated or compared; duplicate behavior is named.
- **row-preserving alias**: a logical second occurrence of one relation, used only to form an explicit self join.
- **ordering metadata**: establishes a required output order but does not claim downstream operators preserve it.
- **row-limiting**: returns at most a declared literal number of ordered rows.

Each output step must still return declared `Schema` instances. No operation infers an output schema from a running
Spark DataFrame.

## Deliberate Phasing

The first generator is `posexplode_struct(...)` over `array<struct>` with `contains_null=False`, because it has a
clear element schema plus an ordinal field and unblocks hierarchy and token expansion. Other generator variants are
separately admitted after their empty/null semantics are tested.

The first composition operations are exact-schema set operations: `union_all`, `union_by_name`, `intersect`,
`intersect_all`, `subtract`, and `except_all`. They permit separate typed branches—such as a global fact branch and a
fallback-context branch—to converge into one declared lane without hiding DataFrame set logic in hooks. Missing-column
union remains deferred because it changes schema-evolution and nullability policy.

A self alias creates independent left/right typed scopes only. It never duplicates data by itself and cannot expose raw
DataFrames. It is useful only with an explicit join/projection boundary.

Ordering requires typed order descriptors or orderable scalar expressions that become ascending descriptors. `limit`
and `offset` use literal non-negative bounds and require the current relation state to still be ordered. Sampling is
deferred because seed, replacement, and reproducibility semantics must be explicit.

## Relation Assertions, Hierarchies, and Priority Selection

The first assertion is deliberately narrower than the later catalog-validation family: `exactly_one(relation)` proves
that one declared relation contains one row before a typed consumer reads it. It preserves the typed scope on success
and lowers through a public aggregate count plus Spark assertion expression, rather than collecting a configuration
row on the driver or choosing an arbitrary first row. Both zero and multiple rows fail with `REL-E0701`. This P0
primitive is batch-only and ordinary-PySpark-only; richer key, predicate, and reference assertions remain P1.

`require_unique(...)`, `require_all(...)`, and `require_reference(...)` are P1 relation assertions. They validate a
declared key, predicate, or nullable parent-reference relationship in the Spark plan and emit registered diagnostics
instead of collecting configuration rows on the driver. They cover band catalog identifiers, age ranges, priority
predicates, and parent existence.

A bounded parent-hierarchy operation accepts typed node identity, parent identity, explicit ordering, literal
`max_depth`, and error policies for missing parents/cycles. It lowers to a finite self-join sequence and yields typed
closure/path rows. It never uses a Python UDF or recursive driver traversal. `hierarchy_fallbacks(...)` derives the
ordered parent-substitution paths from that representation and emits the final global fallback explicitly. Together
with ordered collection, this supports the `BandMembership` and `BandFallback` outputs of `ResolveCohortBands`.
The existing typed self-alias plus anti-existence relation pattern selects leaf matches; it does not need a new
special-purpose hierarchy API.

Priority selection takes a declared stable row key, a candidate relation, an eligibility predicate, and an explicit
priority ordering. It selects at most one qualifying candidate per key, with named missing/tie policy. This avoids
opaque surrogate IDs such as `monotonically_increasing_id()` and supports exact-parent-global fallback selection.

## Alternatives Rejected

- Forward raw PySpark DataFrames through a step method: this erases schemas, cardinality, and traceability.
- Infer output schemas at runtime: this makes no-Spark compilation and generated parity impossible.
- Model every relation operation as a join: set composition and row expansion have different duplicate/cardinality
  semantics.
- Add a generic callback-based relation API: it would be an opaque hook with misleading typed syntax.

## Acceptance Boundary

No relation operation is supported until source validation, immutable recipes, capability checks, online execution,
generated source, explain/traceability, diagnostics, and parity evidence agree. If a Search migration exposes a missing
contract, the hook remains and the gap is recorded in `docs/dev/Gaps.md`.
