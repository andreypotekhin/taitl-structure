# Sprint 46: V9 Contract and Feature Completion

Status: complete; closed 2026-08-02.

## Sprint Goal

Complete the remaining V9 contract and feature decisions after Sprint 45 inventory closure.

## User-Facing Outcome

Finite selected-value streaming alternatives, broad analytic-window boundaries, Variant profile rules, arbitrary-state
requirements, and provider-neutral Geometry have explicit behavior and no hidden semantic decisions.

## Implementation Tasks

- Verify the bounded grouped `first_value(...)`/`last_value(...)` alternative.
- Keep global selected-row and broad analytic-window helpers batch-only for streaming.
- Complete the Variant child-plan profile and exclusion matrix.
- Define the typed arbitrary-state model requirements.
- Complete the provider-neutral Geometry contract without bundling a provider.

## Acceptance and Demo

Run the focused V9 catalog, streaming compatibility, Variant, and Geometry tests. Demonstrate corrective diagnostics for
unsupported streaming shapes and provider/profile gaps.

## Risks and Non-Goals

Do not widen finite selected-value support into general ranking, lag/lead, rolling, lifecycle, or arbitrary-state support.
Live evidence gaps remain explicit and are handed to Sprint 47.

## Governing Plan

`docs/dev/planning/P07302603.V9-closeout-and-release.plan.md`, with child plans P07302601 and P07302602.
