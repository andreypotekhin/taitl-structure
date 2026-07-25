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

The initial helper expands one array-of-struct field into zero or more rows. The author declares a result schema that
contains the selected source fields, an ordinal Long field, and the element struct fields or a declared nested element
field. The exact public helper spelling is chosen during implementation but must make the driving array, ordinal name,
and result schema clear.

- A non-null array with N elements yields N rows in ordinal order 0 through N-1.
- A null or empty array yields zero rows for the inner form.
- An outer form is a separate future admission because its null-row contract differs.
- The operation is row-expanding and batch-only until a streaming contract is separately accepted.
- `explode`, `explode_outer`, `inline`, and `inline_outer` remain deferred in `Gaps.md` until their distinct schemas
  and null/empty semantics have dedicated tests.

## Set Composition

### `union_all`

`union_all(left, right)` requires identical declared schemas including physical names, field types, and nullability
compatibility. It returns every row from both relations and preserves duplicates. Its result has set-combining
cardinality and makes no ordering promise.

### `union_by_name`

`union_by_name(left, right)` aligns fields by declared physical name. V6 initially requires the same field set;
missing-column filling is deferred because it changes nullability and schema evolution semantics. It preserves
duplicates and makes no ordering promise.

`intersect`, `intersect_all`, `subtract`, and `except_all` remain deferred until their distinct/multiset duplicate
rules are separately specified in `Gaps.md`.

## Self Alias

`alias(relation, name=...)` creates a second typed occurrence of one relation for an explicit self join. `name` is a
non-empty literal identifier unique within the step. The alias is row-preserving and does not execute or duplicate
data by itself. A join and explicit schema projection determine the resulting rows.

Field provenance retains the alias occurrence so generated columns and traceability distinguish left and right fields.
Alias collision, self-join predicate ambiguity, and use without an admitted relation consumer fail at compile time.

## Ordering and Selection

`order_by(...)` accepts one or more existing typed order descriptors. It establishes the result presentation order and
is visible at the materialized output boundary. A later operator may not claim to preserve that order unless its own
contract says so.

`limit(n)` and `offset(n)` require non-negative integer literals. `limit` requires a preceding explicit ordering;
`offset` requires a preceding explicit ordering and is supported only where the selected PySpark profile exposes an
equivalent public plan. An unordered bound fails with a determinism diagnostic.

`sample` remains deferred pending seed, replacement, and reproducibility semantics.

## Diagnostics

Diagnostics must cover: undeclared/incompatible schemas, unsupported generator element type, missing result ordinal,
null/outer generator ambiguity, alias collision, invalid self-join use, unaligned union schemas, unsupported set
duplicate policy, absent ordering, negative/nonliteral bounds, unsupported target, and invalid streaming placement.

## Acceptance

- Generator fixtures prove cardinality, ordinal, null, and empty behavior.
- Union fixtures prove schema validation and duplicates.
- Alias fixtures prove independent left/right fields and traceability.
- Ordering/bound fixtures prove determinism diagnostics and output-boundary order.
- Search hook retirement occurs only after the raw and typed forms return equivalent normalized rows in online and
  generated execution.
