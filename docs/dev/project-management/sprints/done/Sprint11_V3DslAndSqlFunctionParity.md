# Sprint 11: v.3 DSL and SQL Function PySpark Parity

## Sprint Goal

Close the planned `DSL` gaps from `docs/dev/Gaps.md` so common scalar Column operations and SQL functions remain
compiler-visible.

## Product Outcome

Developers can write ordinary predicates, casts, ordering, string operations, date/time helpers, numeric helpers, and
predicate helper functions in Structure source without dropping into hooks.

## Scope

### In Scope

- Membership predicates.
- Range predicates.
- String predicates.
- Collection indexing and struct field helpers.
- Rich casts.
- Ordering modifiers including null ordering descriptors needed by later window work.
- Broader string SQL helpers.
- Date/time helpers.
- Numeric/math helpers.
- Predicate helper functions.
- Backend capability checks, diagnostics, explain, traceability, docs, compatibility tables, and parity tests.

### Out of Scope

- Raw SQL string expressions.
- Raw PySpark Column aliases.
- Raw `Column.over(...)`.
- Bitwise methods, struct mutation, and null/NaN expansion unless the gap status changes.
- UDF/UDTF helper admission.

## ExecPlan

`docs/dev/planning/done/P07072602.V3-dsl-and-sql-function-pyspark-parity.plan.md`

## Engineering Tasks

1. Add source helper APIs and symbolic expression records.
2. Add type inference and nullability rules.
3. Add shared PySpark lowering for online and generated execution.
4. Add backend capability diagnostics for target-specific differences.
5. Add docs, compatibility rows, generated examples, and parity tests.

## Acceptance Criteria

- Planned Column API gaps compile to readable PySpark Column operations.
- Planned SQL function gaps compile to readable `pyspark.sql.functions` calls.
- Unsupported raw and opaque expression forms continue to fail before runtime with diagnostic links.
- `make build` passes.

## Progress

- [x] (2026-07-09) Started v.3 implementation as the active iteration.
- [x] (2026-07-09) Added the first v.3 Column helper, inclusive `between(...)` range predicates.
- [x] (2026-07-10) Added `isin(...)` membership predicates with online/generated parity coverage.
- [x] (2026-07-12) Added typed `contains(...)`, `like(...)`, `ilike(...)`, and `rlike(...)` predicates with
  generated and online-recipe parity coverage.
- [x] (2026-07-12) Added typed Array/Map `__getitem__` expressions with inferred result types and shared
  generated/online-recipe parity coverage.
- [x] (2026-07-12) Added scalar `cast(...)`, `astype(...)`, and nullable `try_cast(...)` expressions with shared
  generated/online-recipe parity coverage; `try_cast(...)` is capability-gated to the PySpark 4 profile.
- [x] (2026-07-12) Added typed `asc`, `desc`, and explicit null-ordering descriptors for inline and reusable window
  expressions with shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Added `substring(...)`, `split(...)`, and `regexp_replace(...)` with explicit regex-pattern
  contracts, public API snapshot coverage, and shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Added non-null `concat_ws(...)` with literal separator validation, public API snapshot coverage,
  and shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Added `regexp_extract(...)` with literal Java-regex and capture-group validation, public API
  snapshot coverage, and shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Added typed `length(...)` with String-only validation, public API snapshot coverage, and shared
  generated/online-recipe parity coverage.
- [x] (2026-07-12) Finalized the Sprint 11 String API: `contains`, `like`, `ilike`, `rlike`, `substring`, `split`,
  `regexp_replace`, `regexp_extract`, `length`, and `concat_ws`.
- [x] (2026-07-12) Added typed String transformation, search, and comparison helpers: `initcap`, `reverse`,
  `translate`, `instr`, and `levenshtein`.
- [x] (2026-07-12) Added `date_add(...)`, `datediff(...)`, and `date_trunc(...)` with typed temporal contracts,
  public API snapshot coverage, and shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Added `abs(...)`, `round(...)`, `ceil(...)`, and `floor(...)` with typed numeric contracts,
  public API snapshot coverage, and shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Added `isnull(...)`, `isnotnull(...)`, and typed `isnan(...)` with public API snapshot coverage
  and shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Added alias-aware Struct `get_field(name)` with shared generated/online-recipe parity coverage.
- [x] (2026-07-12) Implemented v.3 DSL and SQL function parity.
