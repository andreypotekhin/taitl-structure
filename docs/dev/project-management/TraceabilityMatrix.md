# Traceability Matrix

This matrix maps early sprints to specification sections and major deliverables.

| Sprint | Spec Areas | Main Deliverables |
|---|---|---|
| Sprint 00 Groundwork | Setup, Configuration, Source Layout, Build Integration, Testing, Pre-Coding Spikes | repo skeleton, CLI skeleton, config loader, CI, safe default package layout, spike notes |
| Sprint 01 Vertical Slice 1 | Schemas, Transform Classes, Inputs, Generated Code, Testing | simple schema, one transform, generated class, Spark execution test |
| Sprint 02 Schemas and Validation | Schema Validation, Generated Code, Configuration | `StructType` generation, `assert_schema`, intermediate validation defaults |
| Sprint 03 Expressions/Filtering/Helpers | Symbolic Execution, Expression Helpers, Filtering, Error Reporting | expression IR, `where`, `@expr_fn`, structured unsupported-code errors |
| Sprint 04 Hooks/Generated Classes | Hooks, Generated Code, Error Reporting | `@after(method)`, direct hook calls, no-hook cleanliness |
| Sprint 05 Joins/Lineage/Build | Joins, Lineage, Build Integration, Streaming Compatibility | `join_one`, N-step joins, LDJSON lineage, `--fail-on-diff`, `explain` |

## Relevant Specification Items by Sprint

### Sprint 00

- As a developer, I can install Structure as a Python package.
- As a developer, I can use `structure_src` and `structure_generated` by default.
- As a developer, I can override defaults with small TOML configuration.
- As a developer, I can run `structure check`.
- As a maintainer, I can review Sprint 00 spike notes before vertical slice coding begins.

### Sprint 01

- As a developer, I can define schema classes.
- As a developer, I can declare a transform class with `@transform`.
- As a developer, I can declare named inputs using `input(Schema)`.
- As a developer, I can generate one PySpark class per transform class.
- As a developer, I can execute generated code against a Spark DataFrame.

### Sprint 02

- As a developer, I can generate Spark `StructType` schemas.
- As a developer, I can validate input schemas.
- As a developer, I can validate intermediate schemas by default.
- As a developer, I can validate final output schemas.
- As a developer, I can disable intermediate validation where configured.

### Sprint 03

- As a developer, I can compile field references to Spark Columns.
- As a developer, I can use `where(...)` for filtering.
- As a developer, I can use `@expr_fn` helpers.
- As a developer, I receive structured compiler errors for unsupported Python.
- As a developer, I receive alternatives including DSL functions, `@expr_fn`, hooks, and config workarounds.

### Sprint 04

- As a developer, I can attach a hook with `@after(method)`.
- As a developer, I can write hook signature `def hook(self, *, df, spark, ctx)`.
- As a developer, generated code directly calls hooks when hooks exist.
- As a developer, hook-free generated code remains clean.

### Sprint 05

- As a developer, I can perform symbolic `join_one(...)` joins.
- As a developer, I can build serial joins across arbitrary numbers of named inputs.
- As a developer, I can generate basic LDJSON lineage.
- As a developer, I can run `structure compile --fail-on-diff` in CI.
- As a developer, generated transforms remain streaming-compatible when Spark supports the operations used.
