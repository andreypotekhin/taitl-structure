# Sprint 45: V9 API Catalog Design Gates

Status: active; target close: 2026-08-07.

## Sprint Goal

Resolve the remaining v9 APICatalog planned and deferred rows with a support-first bias. For the extended streaming
surface, Sprint 45 attempts implementation or executable caller-owned guidance first, and uses `streaming-ineligible` or
continued `design-gated` only when live PySpark evidence or an explicit lifecycle/state contract blocks support.

## User-Facing Outcome

Users can read the public API catalog and see precise support status for streaming gates and non-streaming PySpark API
rows without ambiguous `planned` or `deferred` labels. Implemented streaming rows have working APIs, generated/online
lowering, documentation, diagnostics, compatibility tests, and live target evidence where Spark execution matters.

## In Scope

- Missing-column set composition for `union_by_name(...)`.
- Join reordering design and first supported slice, if the contract stays deterministic.
- PySpark 4 `variant(...)` schema fields, PySpark 4.0/4.2 Variant helpers for migration, and remaining geospatial
  helper type-model decisions.
- Remaining streaming design-gated rows: chained windows, selected-row helpers, finite analytic-window alternatives,
  side-effect APIs, and arbitrary state APIs.
- Live evidence and profile-specific coverage for PySpark 4-only streaming helpers, including Variant fields and Variant
  value helpers.
- Typed `window_time(...)` and the supported two-stage event-time window aggregation shape, with PySpark 3.5/4.0 live
  online/generated evidence.
- Execute the linked [V9 Variant ExecPlan](../../planning/P07302602.V9-variant-type-and-helpers.plan.md): complete the
  released 4.0/4.2 profile matrix, literals/equality, Variant table-valued row expansion, explicit exclusions, and
  evidence closure; keep 4.3+ mutation helpers design-gated until those runtimes are released.
- Public docs, coverage ledgers, diagnostics, and compatibility tests for each resolved row.

## Out of Scope

- XML implementation. XML remains low-priority design-gated work for this sprint.
- Structure-owned streaming lifecycle.
- Raw SQL wrappers for APIs that need typed Structure models first.

## Acceptance

Every v9 API Catalog row touched by this sprint is classified as implemented, unsupported/not-applicable, streaming
ineligible, caller-owned, or design-gated with a linked rationale. Focused tests pass after each implementation slice,
and `make build` passes before the sprint is closed.

## Demo Script

Run the focused catalog and PySpark coverage tests:

```bash
PYTHONPATH=.:src:tests poetry run pytest -q \
  tests/specifications/compatibility/test_api_catalog_design_gates.py \
  tests/specifications/compatibility/test_pyspark_transformation_coverage.py \
  tests/specifications/compatibility/test_pyspark_streaming_api_coverage.py
```

For implemented relation APIs, run the focused specification tests that exercise symbolic capture, generated PySpark,
online execution, diagnostics, and streaming compatibility.

## Risks

- Some Spark APIs are target-version dependent. The sprint should capture both positive live evidence on supported
  profiles and corrective rejection evidence on unsupported profiles before publishing a Structure-supported streaming
  claim.
- Join reordering can silently change semantics unless the first slice is deliberately narrow and explainable.
- Missing-column union can leak ambiguous type/nullability behavior unless fills are explicit in generated source.

## Schedule and Handoff

Sprint 45 is the inventory and decision-closure slice. It hands implementation work to Sprint 46, target evidence and
catalog reconciliation to Sprint 47, and release-only verification to Sprint 48. The dated sequence, dependencies,
fallback rules, and exit criteria are maintained in the
[V9 closeout ExecPlan](../../planning/P07302603.V9-closeout-and-release.plan.md).

Sprint 45 exits when every selected V9 row has an owner, a precise status, a linked specification, and an acceptance
command. It must not consume Sprint 48 by adding new scope after the handoff.

## Governing Documents

`docs/dev/planning/P07302601.V9-api-catalog-design-gates.plan.md`,
`docs/dev/planning/P07302602.V9-variant-type-and-helpers.plan.md`,
`docs/dev/planning/P07302603.V9-closeout-and-release.plan.md`,
`close/archive/decisions/D07302603.V9-streaming-support-first.md`,
`docs/dev/specifications/V9ApiCatalogDesignGatedFeatures.md`, and
`docs/dev/specifications/V9StreamingDesignGatedFeatures.md`.
