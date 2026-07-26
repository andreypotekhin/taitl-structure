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

The first generator is `posexplode` over `array<struct>`, because it has a clear element schema plus an ordinal field
and unblocks hierarchy and token expansion. Other generator variants are separately admitted after their empty/null
semantics are tested.

The first composition operations are `union_all` and exact-schema `union_by_name`. They must permit separate typed
branches—such as a global fact branch and a fallback-context branch—to converge into one declared lane. Distinct set
semantics (`intersect`, `subtract`) and multiset forms are distinct contracts because they treat duplicates differently.

A self alias creates independent left/right typed scopes only. It never duplicates data by itself and cannot expose raw
DataFrames. It is useful only with an explicit join/projection boundary.

Ordering requires typed order descriptors. `limit` and `offset` use literal non-negative bounds; an unordered limit is
rejected. Sampling is deferred because seed, replacement, and reproducibility semantics must be explicit.

## Relation Assertions, Hierarchies, and Priority Selection

`require_unique(...)`, `require_all(...)`, and `require_reference(...)` are relation assertions. They validate a
declared key, predicate, or nullable foreign-key-like relationship in the Spark plan and emit a registered diagnostic
instead of collecting configuration rows on the driver. They are required for band catalog identifiers, age ranges,
parent existence, and parent-priority checks.

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
