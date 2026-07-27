# V6 PySpark API Ledger

## Purpose

This specification defines the release-ledger rules for v6 PySpark API decisions. The ledger tables now live in the
public [API Catalog](../../APICatalog.md), alongside the moved `Gaps.md` API tables. The catalog complements the checked
coverage source and deferred-work register; it does not replace either.

For every catalog row, implementation may begin only after the row has a capability name, diagnostics, source/recipe/
renderer/evaluator owner, test location, and public-reference wording. When a row changes status, update
`docs/APICatalog.md`, `docs/dev/Gaps.md`, and
`src/structure/plugin/pyspark/resources/pyspark-transformation-coverage.json` in the same pull request.

## Status Rules

- **implemented** means supported across its stated target profile with all required evidence.
- **scheduled** means committed to a named v6 sprint and plan.
- **deferred** means the contract is incomplete; `Gaps.md` states the reason and current user boundary.
- **intentional** means Structure will continue to use a hook or caller-owned API because the behavior is not a typed
  data transformation.

## Ledger

The maintained rows are dissolved into the topic tables in [APICatalog.md](../../APICatalog.md), especially
[Relation Operations](../../APICatalog.md#relation-operations), and the detailed Structure additions in
[APIExtensions.md](../../APIExtensions.md). Keep this specification focused on status rules, user boundaries,
raw-hook inventory rules, and required row fields.

## User Boundary

Use a normal step when a ledger operation is implemented. Use an explicit scalar UDF only for deliberately opaque,
row-local Python logic with an honest declared return type/nullability; expect the UDF warning and do not use it with
Spark Connect. Use `@raw` for a narrow target-specific DataFrame transformation while its typed contract is deferred.
Keep sources, sinks, actions, and driver algorithms caller-owned or intentionally raw.

## Example Raw-Hook Inventory

`V6ExampleRawHookInventory.json` is the checked inventory of every `@raw` method under `examples/`. The
`v6-api-ledger` specification test parses the example source and fails when a hook is missing from that inventory or
an inventory entry no longer names a hook.

The current inventory records two retired Security hooks, ten Search hooks scheduled across Sprints 24 and 25, and
intentional School matrix inversion because it is a driver-side numerical algorithm. Each scheduled record names the
smallest capability required for retirement and keeps `@raw` as the boundary until that capability has its complete
typed contract and parity evidence.

## Required Record Fields

Every detailed ledger row added during implementation contains:

```text
capability id; status; public spelling; source PySpark API; operand and result schemas;
null/empty/duplicate/order semantics; cardinality; batch/streaming/Connect support;
diagnostic code; source/recipe/evaluator/renderer owner; example client; public docs;
source, generated, parity, capability, and live test evidence; Gaps.md link when deferred.
```
