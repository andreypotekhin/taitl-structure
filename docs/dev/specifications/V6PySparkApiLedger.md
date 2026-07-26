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
| Partitioned `window_max` | implemented | Sprint 24 | Explicit `WindowSpec` with typed partition/order/frame validation | symbolic, recipe, renderer, evaluator, and type/frame diagnostics |
| Ordered `collect_list` | implemented | Sprint 24 | Explicit ascending/descending key renders as a null-safe sorted collection | compiler, generated-PySpark, and online recipe parity |
| `exactly_one` validation | scheduled | Sprint 25 | Declared relation-cardinality assertion; zero/multiple cardinality diagnostic | recipe/runtime failure evidence |
| Implicit global aggregation | implemented | Sprint 24 | Aggregate-only step works without `group_by`; nullable value aggregates reject non-null output on an empty global level | recipe, renderer, and empty-input schema diagnostics |
| Explicit scalar UDF example | implemented documentation | Sprint 24 | `@special(type="udf")` declares return type/nullability and warns by default | QuickRef, compiler warning, generated expression, traceability, Connect capability rejection |
| `posexplode` over array of structs | scheduled | Sprint 25 | Declared element and ordinal output schema; ExtractText/scoring | cardinality/null/empty parity |
| Other generator forms | deferred | `Gaps.md` / Sprint 25 gate | Admit one by one after distinct contract proof | dedicated specification amendment |
| `union_all` and exact-schema `union_by_name` | scheduled | Sprint 25 | Explicit schema compatibility and duplicate semantics | online/generated duplicate parity |
| Other set operations | deferred | `Gaps.md` | Distinct vs multiset semantics require separate contracts | dedicated specification amendment |
| Self alias | scheduled | Sprint 25 | Two typed occurrences of one relation for explicit self join | provenance and alias collision tests |
| Relation order/limit/offset | scheduled | Sprint 25 | Typed ordering, literal bounds, output-boundary ordering claim | ordering and rejection tests |
| Branchable typed union | scheduled | Sprint 25 | Global and fallback-context branches converge into one lane; relevance client | branch cardinality and parity |
| `require_unique` / `require_all` / `require_reference` | scheduled | Sprint 25 | Spark-plan key, predicate, and nullable parent-reference assertions for band catalogs | diagnostic/runtime parity |
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
