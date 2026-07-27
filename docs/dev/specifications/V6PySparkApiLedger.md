# V6 PySpark API Ledger

## Purpose

This specification is the release ledger for v6 PySpark API decisions. It complements the public coverage catalog and
the deferred-work register; it does not replace either.

For every row below, implementation may begin only after the row has a capability name, diagnostics, source/recipe/
renderer/evaluator owner, test location, and public-reference wording. When a row changes status, update this file,
`docs/dev/Gaps.md`, and `docs/reference/pyspark-transformation-coverage.json` in the same pull request.

## Status Rules

- **implemented** means supported across its stated target profile with all required evidence.
- **scheduled** means committed to a named v6 sprint and plan.
- **deferred** means the contract is incomplete; `Gaps.md` states the reason and current user boundary.
- **intentional** means Structure will continue to use a hook or caller-owned API because the behavior is not a typed
  data transformation.

## Ledger

| Capability | Current v6 status | Owner | Contract and example | Required evidence |
| --- | --- | --- | --- | --- |
| Lambda-bound struct field access | implemented | Sprint 24 | Typed `app.id` inside array/map callback; retired both Security hooks | symbolic, render, generated snapshot, traceability |
| Partitioned `window_max` | implemented | Sprint 24 | Explicit `WindowSpec` with typed partition/order/frame validation | symbolic, recipe, renderer, evaluator, type/frame diagnostics, and Search-shaped partition-isolation source evidence |
| Ordered `collect_list` | implemented | Sprint 24 | Explicit ascending/descending key renders as a null-safe sorted collection | compiler, generated-PySpark, and online recipe parity |
| `exactly_one` validation | implemented | Sprint 24 P0 | Declared relation-cardinality assertion; zero/multiple `REL-E0701` failure; must be declared before the relation is joined | public API, recipe, generated/online lowering, explain, capability, diagnostic, call-order rejection |
| Implicit global aggregation | implemented | Sprint 24 | Aggregate-only step works without `group_by`; nullable value aggregates reject non-null output on an empty global level | recipe, renderer, and empty-input schema diagnostics |
| Explicit scalar UDF example | implemented documentation | Sprint 24 | `@special(type="udf")` declares return type/nullability and warns by default | QuickRef, compiler warning, generated expression, traceability, Connect capability rejection |
| `posexplode` over array of structs | implemented | Sprint 25 | `posexplode_struct(value, as_=..., ordinal=..., scope=...)` expands `array<struct>` with `contains_null=False` into a declared generated scope | source validation, recipe, generated/online lowering, explain, traceability, capability, streaming classification |
| Other generator forms | deferred | `Gaps.md` / Sprint 25 gate | Admit one by one after distinct contract proof | dedicated specification amendment |
| Exact-schema set operations | implemented | Sprint 25 | `union_all(relation)`, `union_by_name(relation)`, `intersect(relation)`, `intersect_all(relation)`, `subtract(relation)`, and `except_all(relation)` combine the active rowset with an unjoined exact-schema relation using Spark's public set semantics | source validation, recipe, generated/online lowering, explain, traceability, capability, streaming classification |
| `relation_alias` self joins | implemented | Sprint 25 | `relation_alias(relation, name=...)` creates a named typed occurrence of the current rowset or an unjoined relation for explicit self joins | source validation, recipe, generated/online join reuse, explain, traceability, capability |
| Relation order/limit/offset | implemented | Sprint 25 | `order_by(...)`, `limit(n)`, and `offset(n)` record relation ordering and deterministic literal bounds; bounds require the current relation state to be ordered | source validation, recipe, generated/online lowering, explain, traceability, capability, streaming classification |
| Branchable typed union | scheduled | Sprint 25 | Global and fallback-context branches converge into one lane; relevance client | branch cardinality and parity |
| `require_unique` / `require_all` / `require_reference` | implemented | Sprint 25 | Spark-plan key, predicate, and nullable parent-reference assertions fail through `REL-E0702`/`REL-E0703`/`REL-E0704` without driver collection | diagnostic/runtime parity |
| Bounded parent hierarchy and fallbacks | scheduled | Sprint 25 | Literal depth plus missing-parent/cycle policies; typed closure/path and ordered parent-substitution fallbacks for `ResolveCohortBands` | closure/path/fallback and invalid-catalog evidence |
| First-qualified priority selection | scheduled | Sprint 25 | Declared row key, eligibility, and fallback order; rerank client | exact/parent/global parity and tie failures |
| Sampling | deferred | `Gaps.md` | Seed/replacement/reproducibility not yet defined | physical-plan specification |
| Bounded ordered `scan(...)` | scheduled | Sprint 26 | Separate typed recurrence plan | Fibonacci parity and live evidence |
| Binary/encoding | deferred | `Gaps.md` | Requires Binary type | type-model specification |
| JSON/CSV parsing | deferred | `Gaps.md` | Requires inline-schema and options transport | parser specification |
| Deterministic `mode` | deferred | `Gaps.md` | PySpark 3.5 baseline lacks needed tie policy | target-policy decision |
| Matrix inversion | intentional raw | School example | Driver-side numerical algorithm | hook inventory record |

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
