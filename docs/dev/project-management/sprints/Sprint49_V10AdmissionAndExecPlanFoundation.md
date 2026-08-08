# Sprint 49: V10 Admission and ExecPlan Foundation

Status: planned; target: 2026-10-02.

## Sprint Goal

Translate the V10 scope into synchronized, self-contained execution plans and implementation-ready catalog rows.

## User-Facing Outcome

Contributors can find one V10 owner, contract, acceptance command, and evidence path for every admitted API or streaming
family.

## Implementation Tasks

- Review the V9 final report and carry forward only bounded follow-ups.
- Publish the four baseline V10 ExecPlans and update governing design/specification/background links; admit the
  collision-safe generated identities plan to the Sprint 54 hardening workstream when a release blocker is found.
- Admit `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md` and freeze its scalar-generator,
  nullability, primitive-type, and Search span contracts.
- Inventory APICatalog rows, capability ledgers, diagnostics, references, and examples.
- Add V10 milestone, backlog, roadmap, traceability, and sprint navigation.

## Acceptance and Demo

All V10 planning links resolve; no application future is accidentally admitted; every selected row has an owner and
status vocabulary; and focused documentation checks pass.

## Risks and Non-Goals

No feature implementation or lifecycle ownership. Do not duplicate one ExecPlan per document.

## Governing Plan

`docs/dev/planning/P08022604.V10-evidence-catalog-reconciliation-and-hardening.plan.md`.
