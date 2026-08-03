# Sprint 47: V9 Evidence, Catalog, and Documentation Reconciliation

Status: complete; closed 2026-08-02.

## Sprint Goal

Collect target evidence and reconcile every V9 claim across implementation, ledgers, diagnostics, generated artifacts,
and public documentation.

## User-Facing Outcome

The API Catalog distinguishes supported, caller-owned, streaming-ineligible, unsupported, and design-gated behavior with
evidence or a named missing contract.

## Implementation Tasks

- Run pinned PySpark 3.5/4.0 online/generated streaming lanes and restart checks.
- Run optional Geometry-provider evidence only when dependencies are pinned.
- Reconcile `docs/APICatalog.md`, API references, capability ledgers, diagnostics, troubleshooting, and generated docs.
- Record unavailable target lanes honestly.

## Acceptance and Demo

Focused compatibility suites pass; every claimed live lane has actually run; generated artifacts are fresh; and the
catalog, specifications, and ledgers agree.

## Risks and Non-Goals

A skipped lane is not a pass. Do not add new API scope or promote a target-gated feature without evidence.

## Governing Plan

`docs/dev/planning/P07302603.V9-closeout-and-release.plan.md`.
