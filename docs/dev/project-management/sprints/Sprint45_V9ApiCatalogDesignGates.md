# Sprint 45: V9 API Catalog Design Gates

Status: complete; closed: 2026-08-02.

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

Sprint 45 closed the inventory and decision-closure slice. The implementation, evidence, and hardening work originally
scheduled for Sprints 46--48 was completed on 2026-08-02 under the umbrella closeout plan. The dated sequence,
dependencies, fallback rules, and exit criteria remain documented in the
[V9 closeout ExecPlan](../../planning/P07302603.V9-closeout-and-release.plan.md).

Sprint 45 exited when every selected V9 row had an owner, a precise status, a linked specification, and an acceptance
command. The final hardening pass added no new API scope.

## Current Closure Inventory

The active inventory now has precise outcomes for the implementation and design slices touched in this sprint:

| Row | Status and owner | Dependency or missing contract | Acceptance command |
| --- | --- | --- | --- |
| `union_by_name(..., allow_missing_columns=True)` | implemented; Structure transform | Nullable top-level batch fills only; defaults, nested fills, and streaming evidence remain out of scope | `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/v6-api-ledger/test_v6_relation_union.py tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py` |
| `streaming.chained-window-aggregation` | structure-supported; Structure transform | Exact watermarked two-stage `window_time(...)` shape; broader stateful chains remain gated | `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py` |
| `streaming.selected-row-helpers` | streaming-ineligible for global relation helpers; batch-materialization boundary | No finite state for global selection; the finite grouped `first_value(...)`/`last_value(...)` alternative has a live integration path pending target-lane execution | `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py` |
| `streaming.analytic-windows` | streaming-ineligible; batch-materialization boundary | Broad ranking, lag/lead, and rolling projections lack a finite state contract | `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py` |
| `streaming.foreach` and arbitrary state | design-gated; future lifecycle/state owner | Sink identity, retries, checkpoints, typed state, timeout, and restart contracts are missing | `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/compatibility/test_pyspark_streaming_api_coverage.py tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py` |
| Variant, Geometry, sampling, nearest as-of, aggregate aliases, and join reordering | implemented, design-gated, or unsupported as recorded in `docs/APICatalog.md`; Structure or future-design owners | Profile-specific live evidence and future provider/optimizer contracts are linked from the catalog rows | `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/compatibility/test_api_catalog_design_gates.py tests/specifications/compatibility/test_pyspark_transformation_coverage.py` |

The focused catalog and compatibility pass currently reports `85 passed`. The remaining live PySpark 3.5/4.0
selected-row execution and restart evidence is handed to Sprint 47; it is not treated as evidence merely because the
integration test exists.

## Governing Documents

`docs/dev/planning/P07302601.V9-api-catalog-design-gates.plan.md`,
`docs/dev/planning/P07302602.V9-variant-type-and-helpers.plan.md`,
`docs/dev/planning/P07302603.V9-closeout-and-release.plan.md`,
`close/archive/decisions/D07302603.V9-streaming-support-first.md`,
`docs/dev/specifications/V9ApiCatalogDesignGatedFeatures.spec.md`, and
`docs/dev/specifications/V9StreamingDesignGatedFeatures.spec.md`.
