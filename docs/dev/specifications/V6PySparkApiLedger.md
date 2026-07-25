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

| Capability | Status at v6 start | Owner | Contract and example | Required evidence |
| --- | --- | --- | --- | --- |
| Lambda-bound struct field access | scheduled | Sprint 24 | Typed `app.id` inside array/map callback; retires both Security hooks | symbolic, render, online/generated parity, live PySpark |
| Partitioned `window_max` | scheduled | Sprint 24 | Explicit `WindowSpec`; normalized BM25 client | type/frame diagnostics, two-partition parity |
| Ordered `collect_list` | scheduled | Sprint 24 | Explicit ordering and deterministic collection; similarity-query client | shuffled-input deterministic parity |
| `exactly_one` validation | scheduled | Sprint 24 | Zero/multiple cardinality diagnostic | recipe/runtime failure evidence |
| Implicit global aggregation | scheduled | Sprint 24 | Aggregate-only step works without `group_by`; Index summaries | empty-input and non-aggregate rejection parity |
| Explicit scalar UDF example | scheduled documentation | Sprint 24 | `@special(type="udf")` with declared type/nullability and warning | ordinary-PySpark generated/online example; Connect rejection |
| `posexplode` over array of structs | scheduled | Sprint 25 | Declared element and ordinal output schema; ExtractText/scoring | cardinality/null/empty parity |
| Other generator forms | deferred | `Gaps.md` / Sprint 25 gate | Admit one by one after distinct contract proof | dedicated specification amendment |
| `union_all` and exact-schema `union_by_name` | scheduled | Sprint 25 | Explicit schema compatibility and duplicate semantics | online/generated duplicate parity |
| Other set operations | deferred | `Gaps.md` | Distinct vs multiset semantics require separate contracts | dedicated specification amendment |
| Self alias | scheduled | Sprint 25 | Two typed occurrences of one relation for explicit self join | provenance and alias collision tests |
| Relation order/limit/offset | scheduled | Sprint 25 | Typed ordering, literal bounds, output-boundary ordering claim | ordering and rejection tests |
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

## Required Record Fields

Every detailed ledger row added during implementation contains:

```text
capability id; status; public spelling; source PySpark API; operand and result schemas;
null/empty/duplicate/order semantics; cardinality; batch/streaming/Connect support;
diagnostic code; source/recipe/evaluator/renderer owner; example client; public docs;
source, generated, parity, capability, and live test evidence; Gaps.md link when deferred.
```
