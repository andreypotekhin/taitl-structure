# Sprint 23: V6 API Ledger and PySpark Plugin Decomposition

## Sprint Goal

Make the remaining PySpark transformation frontier reviewable and make its implementation boundaries small enough to
change safely.

## Product Outcome

Maintainers and users can see why every remaining API or example raw hook is supported, scheduled, deferred, or
intentional. Subsequent v6 features land in focused PySpark components without changing the public DSL or unrelated
renderer/executor behavior.

## Scope

### In Scope

- Publish the v6 API ledger and raw-hook inventory derived from the transformation coverage catalog and examples;
  synchronize every postponed/deferred disposition with `docs/dev/Gaps.md`.
- Characterize generated, recipe, online, traceability, and public-import behavior before each extraction.
- Extract focused delegates from the oversized PySpark operation, expression, scope, result, evaluation, execution,
  rendering, and traceability modules.
- Preserve the PySpark public façade and existing endpoint-only cross-app boundary.
- Create executable fixtures for Security and Search migration prerequisites.

### Out of Scope

- New public transformation helpers or changes to PySpark semantics.
- Replacing a raw hook.
- Revising the v5 Plugin API or accepting a private Core import from PySpark.

## Governing Plan

`docs/dev/planning/P07242604.V6-pyspark-api-and-example-hook-retirement.plan.md`

## Acceptance Criteria

- The ledger gives every deferred/unsupported catalog item and every example `@raw` method a status, rationale,
  owner sprint, contract link, and evidence location.
- `docs/dev/Gaps.md`, the coverage catalog, and the ledger agree on every v6 postponed/deferred status.
- Extracted components have one coherent responsibility and direct characterization coverage.
- Generated snapshots, public imports, online recipes, and traceability remain behaviorally identical.
- No new PySpark module imports a private `structure.core` implementation or another PySpark app's `commands`/`logic`
  package.
- `make build` passes.

## Risks and Controls

- Extraction drift: make one focused extraction per change, compare generated artifacts, and run parity tests before
  any semantic change.
- Decorative catalog work: the raw-hook inventory test makes ledger omissions fail rather than relying on prose.

## Progress

- [ ] Publish ledger and raw-hook inventory contract.
- [ ] Add characterization fixtures.
- [ ] Extract focused delegates with no behavior change.
- [ ] Run release regression evidence.
