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
- [x] (2026-07-26) Added the parent hierarchy validation slice:
  `require_parent_hierarchy(id, parent=..., order_by=..., max_depth=...)` checks bounded catalogs for missing parents,
  cycles, depth overruns, and non-increasing child order through Spark-visible `REL-E0706` assertions. Typed
  closure/path rows and fallback expansion remain the next cohort-band slice.
- [x] (2026-07-26) Added bounded hierarchy closure rows:
  `hierarchy_closure(id, parent=..., as_=..., max_depth=...)` replaces the active relation with typed
  `(node, ancestor, depth)` rows through finite public self-join expansion. Fallback expansion remains the remaining
  cohort-band relation slice before `ResolveCohortBands` migration.
- [x] (2026-07-27) Added bounded hierarchy fallback expansion:
  `hierarchy_fallbacks(source_id, path, parents, parent_id=..., parent=..., as_=..., max_depth=...)` emits ordered
  fallback IDs and the terminal global fallback row through public Spark path expansion. `ResolveCohortBands` still
  waits for same-fixture migration before raw-hook retirement.
- [x] (2026-07-27) Locked the typed Search cohort matcher prerequisite:
  the real `User`/`Band` schema shape compiles and renders the raw hook's wildcard-or-membership predicate through
  `cross_join(...)`, `where(...)`, `size(...)`, and `array_contains(...)`, leaving `ResolveCohortBands` blocked only on
  the remaining same-fixture migration work rather than this predicate vocabulary.
- [x] (2026-07-27) Retired `ScoreOverlap.score_overlap`:
  query normalization now expands typed token structs with `arr_transform(...)` and `posexplode_struct(...)`, de-dupes
  terms in a lane, computes per-query term counts, joins reusable index terms by token, and projects the four overlap
  score outputs without an opaque raw hook.
- [x] (2026-07-27) Retired `ScoreBm25.score_bm25`:
  `ScoreBase` now owns shared typed query-term expansion, while BM25 joins each reusable index grain to one-row
  summaries and computes grouped `sum(log(...))` score outputs through compiler-visible PySpark DSL operations.
- [x] (2026-07-27) Retired `CreateSimilarityQueries.build`:
  policy rows now pass `require_all(...)` and `exactly_one(...)` checks, per-grain query text is built with ordered
  `collect_list(...)`, and the final `SearchQuery` relation is an exact-schema typed union of the four grain lanes.
- [x] (2026-07-27) Retired `ReduceSimilarityScores.reduce`:
  directed scoring evidence now flows through typed lanes, reciprocal candidates are matched with named self aliases,
  canonical pairs are mirrored through exact-schema `union_all(...)`, and per-source ranking stays Spark-visible.
- [x] (2026-07-27) Retired `BuildRelevanceSignals.expand_impressions` and `.expand_clicks`:
  global, user-band fallback, and direct-band context rows are now explicit typed branches that merge through
  exact-schema `union_all(...)`.
- [x] (2026-07-27) Retired composed `RerankDocuments.score_candidates`:
  candidate fallback options, first-qualified query/popularity feedback, and policy-weighted feedback scoring now stay
  in typed lanes without a raw surrogate row identifier.
- [x] (2026-07-27) Retired `CreateIndex.build`:
  term counts, target statistics, token document frequencies, final term rows, and aggregate-only summaries are now
  compiler-visible typed steps.
- [x] (2026-07-27) Retired `ResolveCohortBands.resolve_bands`:
  bounded band validation, wildcard-or-membership matching, leaf pruning, lineage closure, reusable user-band catalog
  publication, user-band memberships, and fallback chains now stay in compiler-visible typed lanes without driver
  collection.
- [x] (2026-07-27) Retired `ExtractText.extract`:
  document lines, paragraph lines, section headings, paragraph content, sentences, and words now expand through typed
  struct generators, windowed cumulative grouping, ordered paragraph-line collection, and ordinary projection lanes.
- [ ] Specify remaining generator and relation-composition contracts.
- [ ] Implement remaining relation plans and end-to-end parity paths.
- [ ] Migrate Search slices in dependency order.
- [ ] Update hook ledger and run regression evidence.
