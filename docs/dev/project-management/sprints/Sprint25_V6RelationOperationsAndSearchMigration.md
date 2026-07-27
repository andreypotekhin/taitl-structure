# Sprint 25: V6 Typed Relation Operations and Search Migration

## Sprint Goal

Admit the smallest schema-aware relation operations needed to replace Search's DataFrame hooks, while preserving
cardinality, duplicates, ordering, and multi-output semantics visibly in Structure's plans.

## Product Outcome

Search transforms that currently hide standard DataFrame transformations in raw methods become ordinary, reviewed
step-method pipelines when their exact typed contracts are available. Any remaining Search hook has a documented
reason rather than an accidental API gap.

## Scope

### In Scope

- Typed `posexplode` over `array<struct>` first, followed only by generator variants with separately proven semantics.
- Exact-schema `union_all`, `union_by_name`, `intersect`, `intersect_all`, `subtract`, and `except_all`.
- Named self-alias scopes for explicit self joins.
- Typed relation `order_by`, literal `limit`, and literal `offset`.
- Branchable typed union, relation assertions including parent references, bounded parent-hierarchy closure with
  deterministic fallback expansion, and declared-key first-qualified
  priority selection.
- Slice-by-slice migrations for ExtractText, overlap/BM25 scoring, index summaries, similarity queries, similarity
  reduction, relevance-context expansion, document reranking, and cohort-band resolution after each prerequisite
  exists.
- Raw-versus-typed equivalence fixtures with deterministic row normalization.

### Out of Scope

- `sample`, repartitioning, coalescing, checkpointing, storage, actions, and streaming lifecycle APIs.
- A fake universal relation API that erases output schema or cardinality.
- Removing a hook before same-fixture online/generated equivalence passes.

## Governing Plan

`docs/dev/planning/P07242604.V6-pyspark-api-and-example-hook-retirement.plan.md`

## Acceptance Criteria

- Every relation helper declares its input relation, output schema(s), cardinality, null/empty behavior, duplicate
  behavior, ordering guarantee, and streaming/Connect classification.
- Generator fixtures prove output row count and ordinal semantics; set fixtures prove duplicate behavior; alias fixtures
  prove left/right identity; ordering fixtures prove output-boundary behavior.
- Each retired Search hook and adapter is deleted only after raw-versus-typed output equivalence plus normal parity
  evidence.
- Search hooks that remain are listed in the ledger with a P2 rationale and no misleading “supported” claim.
- `make build` passes.

## Risks and Controls

- Relation operations can produce accidental cardinality changes: require a `OperationPlan` cardinality declaration
  and traceability proof before lowering.
- Search's multi-output hooks can hide separate logical stages: split them into explicit step boundaries instead of
  manufacturing placeholder rows.

## Progress

- [x] (2026-07-26) Implemented the first typed generator slice:
  `posexplode_struct(value, as_=..., ordinal=..., scope=...)` over `array<struct>` with `contains_null=False`.
  The operation records row-expanding cardinality, maps to immutable recipes, renders public PySpark
  `F.posexplode`, runs online through the same recipe, reports explain/traceability facts, and is classified
  batch-only for streaming compatibility.
- [x] (2026-07-26) Repository gate passed after the generator slice: 1,195 tests passed and 29 live-PySpark tests
  skipped; the release subset passed 34 tests with 6 intentional live-test skips, and source and wheel distributions
  built successfully.
- [x] (2026-07-26) Implemented exact-schema relation composition: `union_all(relation)` and
  `union_by_name(relation)` append an unjoined relation to the active rowset, preserve duplicates, reject unaligned
  schemas, lower through immutable recipes, render public PySpark union calls, run online through the same path, and
  report explain/traceability/streaming facts.
- [x] (2026-07-26) Completed the typed set-composition slice: `intersect(relation)`,
  `intersect_all(relation)`, `subtract(relation)`, and `except_all(relation)` reuse the exact-schema relation-set
  contract, preserve Spark's distinct/multiset semantics by lowering to public DataFrame set methods, and carry through
  generated source, online execution, explain, traceability, capabilities, and streaming diagnostics.
- [x] (2026-07-26) Repository gate passed after the set-composition slice: 1,216 tests passed and 29 live-PySpark tests
  skipped; the release subset passed 34 tests with 6 intentional live-test skips, and source and wheel distributions
  built successfully.
- [x] (2026-07-26) Implemented the self-alias join foundation: `relation_alias(relation, name=...)` records a
  compiler-visible alias scope for the current rowset or an unjoined relation, reuses existing typed joins against the
  same source frame, rejects pre-join alias field reads, and reports explain/traceability/capability facts. Search
  similarity reduction still waits for ranked selection before hook retirement.
- [x] (2026-07-26) Repository gate passed after the self-alias slice: 1,220 tests passed and 29 live-PySpark tests
  skipped; the release subset passed 34 tests with 6 intentional live-test skips, and source and wheel distributions
  built successfully.
- [x] (2026-07-26) Implemented deterministic relation ordering and bounded selection: `order_by(...)`, `limit(n)`,
  and `offset(n)` are compiler-visible relation operations with literal-bound validation, generated and online PySpark
  lowering, explain/traceability/capability records, batch-only streaming diagnostics, and a guard that rejects bounds
  after an order-destroying relation operation.
- [x] (2026-07-26) Implemented the first P1 relation assertions: `require_unique(keys...)` and
  `require_all(predicate)` preserve rows on success, fail through Spark-visible assertion expressions with
  `REL-E0702`/`REL-E0703`, lower through generated and online execution, and report explain/traceability/capability
  and streaming facts. `require_reference(...)` remains scheduled for nullable parent-reference checks.
- [x] (2026-07-26) Completed the P1 relation assertion family with `require_reference(value, reference,
  reference_key=..., nulls="allow")`. It validates nullable parent/foreign-key-like values against an unjoined
  reference relation through public Spark projections, duplicate removal, left-anti join, and assertion expressions,
  reports `REL-E0704`, and keeps generated/online/explain/traceability/capability/streaming evidence compiler-visible.
- [x] (2026-07-26) Implemented declared-key first-qualified priority selection:
  `select_first_qualified(keys..., where=..., order_by=..., missing="allow")` filters eligible candidates, validates
  configured missing/tie failures with `REL-E0705`, selects one candidate per business key through public
  `row_number()` window lowering, and reports generated/online/explain/traceability/capability/streaming evidence.
  Search document reranking still waits for same-fixture migration before hook retirement.
- [x] (2026-07-26) Closed the branchable typed-union P1 contract: independently materialized typed lanes can feed a
  later `union_all(...)` step, generated code unions the materialized lane frames, and traceability records the
  branch-to-branch dependency. Search relevance-context expansion still waits for same-fixture migration before hook
  retirement.
- [ ] Specify remaining generator and relation-composition contracts.
- [ ] Implement remaining relation plans and end-to-end parity paths.
- [ ] Migrate Search slices in dependency order.
- [ ] Update hook ledger and run regression evidence.
