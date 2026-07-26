# Sprint 24: V6 Compiler-Visible Security Reconciliation

## Sprint Goal

Replace the Security example's two raw reconciliation hooks with typed step methods, proving that the DSL can express
the underlying Spark plan rather than merely wrapping it.

## Product Outcome

Security reconciliation remains optimizer-visible in online and generated PySpark. Users can inspect its fields,
filters, and dependencies in explain/traceability output instead of encountering opaque raw-hook boundaries.

## Scope

### In Scope

- Typed field access on lambda-bound struct values in collection callbacks.
- Explicit partitioned analytic maximum helper with typed window rules.
- Deterministic ordered aggregate collection, exactly-one validation, and global aggregate contracts, including
  empty-input behavior, while preserving aggregate-only methods without a preceding `group_by(...)` call.
- A small documented example of the existing opt-in scalar `@special(type="udf")` contract, including return type,
  nullability, ordinary-PySpark warning behavior, and Spark Connect exclusion.
- Recipe, evaluator, renderer, capability, diagnostic, explain, and traceability support for each admitted helper.
- Refactoring `retain_reconciled_inventory` and `reconcile_device_inventory` to ordinary Security step methods.

### Out of Scope

- Generators, relation union, self aliases, and Search migration.
- Raw `WindowSpec`, arbitrary callback behavior, implicit Python UDF lowering, or a raw DataFrame escape hatch.

## Governing Plan

`docs/dev/planning/P07242604.V6-pyspark-api-and-example-hook-retirement.plan.md`

## Acceptance Criteria

- `arr_exists(..., lambda app: app.id == software_id)` compiles, runs online, and renders readable generated PySpark
  with exact type/nullability/physical-name preservation.
- Security returns the same matching and nonmatching rows through online and generated execution, with no raw hook
  entry in its compiled traceability.
- Window and aggregate helpers have explicit partition/order/cardinality/empty-input failure diagnostics and parity
  tests across supported profiles.
- The UDF example proves the user-authored scalar UDF path is supported and warning-governed, not a substitute for a
  missing symbolic operation.
- The hook inventory marks both Security hooks retired; placeholder expressions used solely to bridge the raw hook are
  deleted.
- `make build` passes.

## Risks and Controls

- Lambda fields might be rendered as relation fields: test nested aliases, nullable structs, and non-struct failures.
- Ordered collection could accidentally depend on Spark's unordered aggregate: require explicit ordering and repeat
  shuffled-input tests.

## Progress

- [x] (2026-07-26) Confirmed and characterized lambda-bound struct field access through `arr_exists(...)`: typed
  `app.id` preserves its physical field name, lowers to the shared expression recipe, and renders as
  `F.exists(..., lambda app: app.getField('id') ...)`.
- [x] (2026-07-26) Replaced both Security raw reconciliation hooks with typed steps. The raw inventory marks them
  retired; compiler traceability reports no opaque boundary; regenerated Security artifacts preserve the typed plan.
- [x] (2026-07-26) Full repository gate passed: 1,178 tests passed and 29 live-PySpark tests skipped; the release
  subset passed 34 tests with 6 intentional live-test skips, and source and wheel distributions built successfully.
- [x] (2026-07-26) Added deterministic `collect_list(..., order_by=...)`. The compiler retains explicit ascending or
  descending order, while generated and online PySpark collect non-null structs, sort them, and project the values.
- [x] (2026-07-26) Verified and recorded existing partitioned `window_max` and aggregate-only global aggregation
  contracts, including empty-input nullability diagnostics. Documented the existing explicit scalar UDF boundary,
  warning policy, generated embedding option, and Spark Connect exclusion in QuickRef.
- [x] (2026-07-26) Deferred `exactly_one` validation to Sprint 25. The only current client checks policy cardinality
  with a driver action; a correct replacement needs the already-scheduled declared relation-assertion runtime contract
  and must not introduce a hidden action or a nondeterministic `first` fallback.
- [x] (2026-07-26) Recorded the live-evidence limitation: no supported local PySpark target was available, so the
  repository gate retained 29 intentional live-test skips. Spark-free compiler, generated-source, traceability,
  golden, and reference-contract evidence passed.
