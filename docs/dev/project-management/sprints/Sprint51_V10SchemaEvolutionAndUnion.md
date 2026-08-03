# Sprint 51: V10 Schema Evolution and Missing-Column Union

Status: planned; target: 2026-10-30.

## Sprint Goal

Deliver or explicitly gate typed defaults, nested-struct evolution, alias preservation, and streaming union behavior.

## Progress Snapshot

As of 2026-08-02, top-level scalar defaults are implemented through symbolic validation, generated PySpark, online
execution, alias-preserving rendering, and traceability. Nested-struct defaults and live streaming evidence remain
open sprint work.

## User-Facing Outcome

Callers can evolve compatible schemas without losing nullability, aliases, or generated-code readability, and receive
actionable diagnostics for unsafe evolution.

## Implementation Tasks

- Implement `union_by_name(..., defaults=...)` using canonical field paths and typed literals.
- Support nullable and explicitly defaulted nested structs; reject implicit array/map element evolution.
- Verify online/generated parity, schema materialization, diagnostics, and traceability.
- Run streaming evidence before changing the streaming ledger.

## Acceptance and Demo

Focused union, schema, generated-source, and streaming-classification tests pass; non-nullable missing fields without
defaults fail clearly.

## Risks and Non-Goals

Do not introduce a second schema-evolution API or claim streaming support without live evidence.

## Governing Plan

`docs/dev/planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md`.
