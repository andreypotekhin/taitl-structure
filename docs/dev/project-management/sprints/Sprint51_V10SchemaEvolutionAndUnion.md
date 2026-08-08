# Sprint 51: V10 Schema Evolution and Missing-Column Union

Status: planned; target: 2026-10-30.

## Sprint Goal

Deliver or explicitly gate typed defaults, nested-struct evolution, alias preservation, and streaming union behavior.

## Progress Snapshot

As of 2026-08-02, top-level scalar defaults and nested struct defaults are implemented through symbolic validation,
generated PySpark, online execution, alias-preserving rendering, and traceability. Live streaming evidence remains open.

## User-Facing Outcome

Callers can evolve compatible schemas without losing nullability, aliases, or generated-code readability, and receive
actionable diagnostics for unsafe evolution.

## Implementation Tasks

- Implement `union_by_name(..., defaults=...)` using canonical field paths and typed literals.
- Support nullable and explicitly defaulted nested structs; reject implicit array/map element evolution.
- Verify online/generated parity, schema materialization, diagnostics, and traceability.
- Run streaming evidence before changing the streaming ledger.
- Migrate Search line chunking to the scalar generator contract and establish generated/online multilingual span parity under
  `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`.

## Acceptance and Demo

Focused union, schema, generated-source, and streaming-classification tests pass; non-nullable missing fields without
defaults fail clearly.

## Risks and Non-Goals

Do not introduce a second schema-evolution API or claim streaming support without live evidence.

## Governing Plan

`docs/dev/planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md`.
