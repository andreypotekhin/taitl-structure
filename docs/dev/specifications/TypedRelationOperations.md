# Typed Relation Operations

## Purpose

This specification admits the first v6 operations that act on whole relations rather than only the current row. Each
operation has a declared schema and cardinality contract so it can participate in normal PySpark compilation,
execution, generated source, diagnostics, explain, and traceability.

## Common Rules

- Relation operations may consume only declared inputs, lanes, or a prior compiler-visible relation result.
- Output schemas are declared in Structure source; runtime Spark schemas are never inferred as the source contract.
- Every operation has an immutable PySpark body/recipe record, a capability name, source provenance, and registered
  diagnostics.
- An operation must state cardinality, null/empty behavior, duplicate behavior, ordering behavior, and
  batch/streaming/Spark Connect classification.
- A relation operation cannot occur inside a scalar lambda callback, aggregate assignment, or window expression unless
  a later specification explicitly admits that composition.
- Online and generated implementations use the same recipe and public PySpark APIs; neither path may hide an action,
  RDD/Pandas operation, raw SQL, or implicit UDF fallback.

## Generators

### First admitted shape: `posexplode` over `array<struct>`

The initial helper, `posexplode_struct(value, as_=GeneratedScope, ordinal="ordinal", scope="...")`, expands one
array-of-struct field into zero or more rows. The author declares a generated scope schema that contains an ordinal
Long field and the element struct fields. The final output schema can combine the original input fields with fields
read from that generated scope through normal Structure projection.

- A non-null array with N elements yields N rows in ordinal order 0 through N-1.
- A null or empty array yields zero rows for the inner form.
- Nullable array elements are rejected until their flattening/nullability semantics are admitted; declare
  `contains_null=False`.
- An outer form is a separate future admission because its null-row contract differs.
- The operation is row-expanding and batch-only until a streaming contract is separately accepted.
- `explode`, `explode_outer`, `inline`, and `inline_outer` remain deferred in `Gaps.md` until their distinct schemas
  and null/empty semantics have dedicated tests.

## Set Composition

### `union_all`

`union_all(relation)` appends an unjoined relation to the active rowset. The active rowset and the relation must have
identical declared schemas including field order, physical names, field types, and nullability. It returns every row
from both relations and preserves duplicates. Its result has set-combining cardinality and makes no ordering promise.

### `union_by_name`

`union_by_name(relation)` appends an unjoined relation to the active rowset and aligns fields by declared physical
name. V6 initially requires the same field set and the same declared schema; missing-column filling is deferred because
it changes nullability and schema evolution semantics. It preserves duplicates and makes no ordering promise.

### `intersect` and `intersect_all`

`intersect(relation)` keeps rows that appear in both exact-schema relations and follows Spark's distinct set
semantics. `intersect_all(relation)` keeps duplicate occurrences according to Spark's multiset semantics.

### `subtract` and `except_all`

`subtract(relation)` removes rows that appear in the right relation using Spark's distinct EXCEPT semantics.
`except_all(relation)` removes rows using Spark's duplicate-preserving multiset semantics.

All admitted set operations require identical declared schemas including field order, physical names, field types, and
nullability. They are batch-only, make no ordering promise, and must run before joins, generators, aggregation, or
selected-row operations in the same step.

## Self Alias

`relation_alias(relation, name=...)` creates a second typed occurrence of the current rowset or an unjoined relation for
an explicit self join. `name` is a non-empty public Python identifier unique within the step. The alias is
row-preserving and does not execute or duplicate data by itself. A join and explicit schema projection determine the
resulting rows.

Field provenance retains the alias occurrence so generated columns and traceability distinguish left and right fields.
Alias collision, self-join predicate ambiguity, and reading alias fields before joining the alias fail at compile time.

## Ordering and Selection

`order_by(...)` accepts one or more typed order descriptors, or orderable scalar expressions that are interpreted as
ascending order descriptors. It establishes the current relation presentation order and is visible at the materialized
output boundary. A later operator may not claim to preserve that order unless its own contract says so.

`limit(n)` and `offset(n)` require non-negative integer literals. Both require the current relation state to have a
preceding explicit `order_by(...)`; if a row-shaping or set operation has run after the latest ordering, the bound is
rejected as nondeterministic.

`sample` remains deferred pending seed, replacement, and reproducibility semantics.

## Branchable Union

Two or more typed relation branches may converge through `union_all` into one declared lane when every branch returns
the same declared schema. This supports a global branch plus one row for every qualifying fallback context. Each branch
retains source provenance; union preserves duplicates and makes no ordering claim.

## Relation Assertions

### `exactly_one(relation)`

`exactly_one(relation)` is the narrow P0 relation-cardinality assertion. It requires that the declared input relation
contain exactly one row and returns the same typed relation on success, so later normal typed joins may read its
fields. Zero rows and more than one row fail at Spark evaluation with one registered cardinality diagnostic; neither
case is converted to a null row, silently filtered result, driver `collect()`, or nondeterministic `first` value.

The initial implementation is batch-only and ordinary-PySpark-only. It records the asserted relation and its source
provenance in the immutable operation recipe and traceability. Generated and online paths use a public aggregate
count plus assertion expression and no Python action. It is forbidden in scalar lambdas, aggregate assignments,
windows, and streaming steps. `CreateSimilarityQueries` is intended to replace its driver cardinality check with this
primitive before reading `SimilarityPolicy.max_document_frequency_ratio`.

`require_unique(keys...)` fails at Spark evaluation when two rows share the declared key. `require_all(predicate)`
fails when any row does not satisfy the symbolic predicate; null predicate results are failures.
`require_reference(value, reference, reference_key=..., nulls="allow")` fails when a checked value has no declared
reference row. Null checked values pass by default; `nulls="reject"` treats them as violations.

All three assertions preserve the current rowset on success, record their source relation and constraint in
explain/traceability, and are batch-only until a streaming validation contract is defined.

## Bounded Parent Hierarchy

`hierarchy_closure(...)` accepts a node relation, literal `max_depth`, node id, parent id, and explicit ordering. It
returns typed `(node_id, ancestor_id, depth)` closure rows and an ordered path representation. Missing parent, cycle,
and depth-overrun policies are `ERROR` in v6. Implementation uses a finite chain of self joins; it must not use a
driver action, Python UDF, or recursive Spark extension.

`hierarchy_fallbacks(paths, parent=..., ordinal=...)` expands each declared ordered band path by replacing its final
band with its parent until no parent remains, then emits one terminal empty/global path. It returns one row per input
path and ordinal, preserves the canonical ordered band-id array for a subsequent symbolic digest, and makes no
implicit ordering claim beyond the explicit ordinal. `ResolveCohortBands` uses closure/path, ordered collection,
`posexplode`, and this operation to produce its `BandMembership` and `BandFallback` outputs. Its leaf-match query
uses the existing self-alias plus anti-existence relation pattern; no new raw DataFrame operation is required.

## First-Qualified Priority Selection

`select_first_qualified(keys..., where=..., order_by=..., missing="allow", ties=TiePolicy.ERROR)` accepts stable
declared row keys on the current candidate relation, an eligibility predicate, an ordered priority expression, and
explicit missing/tie policies. It yields at most one selected candidate per key. `missing="error"` fails when a key has
no eligible row; `missing="allow"` drops such keys. Ties on the same key and priority value fail with `REL-E0705`.

Keys must be declared field references sourced from business fields; generated surrogate IDs such as
`monotonically_increasing_id()` are not admitted. The operation lowers to Spark-visible key projection, eligibility
filtering, aggregate assertions, and `row_number()` ranking, and records candidate and selected sources in traceability.
`RerankDocuments` uses it to choose exact, parent, then global feedback.

## Diagnostics

Diagnostics must cover: undeclared/incompatible schemas, unsupported generator element type, missing result ordinal,
null/outer generator ambiguity, alias collision, invalid self-join use, unaligned union schemas, unsupported set
duplicate policy, absent ordering, negative/nonliteral bounds, nonunique assertion keys, failed relation predicates,
missing reference, missing parent, cycle, hierarchy-depth overrun, malformed hierarchy fallback input, unstable priority key, ambiguous/missing priority candidate,
unsupported target, and invalid streaming placement.

## Acceptance

- Generator fixtures prove cardinality, ordinal, null, and empty behavior.
- Union fixtures prove schema validation and duplicates.
- Alias fixtures prove independent left/right fields and traceability.
- Ordering/bound fixtures prove determinism diagnostics and output-boundary order.
- Search hook retirement occurs only after the raw and typed forms return equivalent normalized rows in online and
  generated execution.
