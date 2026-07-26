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
- Exact-schema `union_all` and `union_by_name`, then separately tested set/multiset forms where required.
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

- [ ] Specify generator and relation-composition contracts.
- [ ] Implement relation plans and end-to-end parity paths.
- [ ] Migrate Search slices in dependency order.
- [ ] Update hook ledger and run regression evidence.
