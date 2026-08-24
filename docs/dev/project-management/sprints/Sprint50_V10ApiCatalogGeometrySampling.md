# Sprint 50: V10 API Catalog Contracts, Geometry, and Sampling

Status: completed; closeout: 2026-08-22; planned target: 2026-10-16.

Closeout: Geometry, sampling, scalar generators, and the selected API Catalog dispositions were implemented or
explicitly gated. Optional-provider and live target evidence remain outside the local closeout evidence.

## Sprint Goal

Complete implementation-ready contracts for Geometry, sampling, XML, Variant mutation profiles, and join reordering.

## User-Facing Outcome

Users can rely on provider-neutral Geometry and explicit sampling behavior and can see precise remedies for gated API
families.

## Implementation Tasks

- Verify Geometry SRID, WKT, nullability, capability, and provider boundaries.
- Preserve sampling reproducibility and batch-only streaming classification.
- Define bounded dispositions for XML and future Variant profiles.
- Prototype opt-in, explainable inner-equality join reordering or retain the explicit gate.
- Implement the typed scalar-array IR/DSL contract and update generated, online, explain, and API-ledger surfaces under
  `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`.

## Acceptance and Demo

Symbolic, generated, diagnostic, compatibility, and optional-provider tests pass or document exact target gaps.

## Risks and Non-Goals

No bundled Sedona dependency, raw `ST_*` escape hatch, dynamic SRID, or silent optimizer reordering.

## Governing Plan

`docs/dev/planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md`.
