# Traceability Matrix

This matrix maps early sprints to specification sections and major deliverables.

| Sprint | Spec Areas | Main Deliverables |
|---|---|---|
| Sprint 00 Groundwork | Setup, Configuration, Compatibility, Source Layout, Build Integration, Testing, Spikes | repo skeleton, CLI skeleton, config loader, compatibility policy, CI, source-root discovery, spike notes |
| Sprint 01 Vertical Slice 1 | Schemas, Transform Classes, Inputs, Online Execution, Generated Code, Testing | simple schema, one transform, online runner, generated class, Spark execution test |
| Sprint 02 Schemas and Validation | Schema Validation, Generated Code, Configuration | `StructType` generation, `assert_schema`, intermediate validation defaults |
| Sprint 03 Expressions/Filtering/Helpers | Symbolic Execution, Expression Helpers, Filtering, Error Reporting | expression IR, `where`, `@expr_fn`, diagnostic registry, structured unsupported-code errors |
| Sprint 04 Hooks/Generated Classes | Hooks, Generated Code, Error Reporting | `@after(method)`, direct hook calls, no-hook cleanliness |
| Sprint 05 Joins/Compiler Lineage/Build | Joins, Compiler Lineage, Build Integration, Streaming Compatibility | `join_one`, N-step joins, compiler provenance, static dataflow lineage, `--fail-on-diff`, `explain` |
| Sprint 07 Analytical Join Coverage | Analytical Joins, Backend Capabilities, Lineage, Streaming Compatibility | existence joins, `join_many`, deterministic lookup dedupe, temporal joins, as-of joins |

## Relevant Specification Items by Sprint

### Sprint 00

- As a developer, I can install Structure as a Python package.
- As a developer, I can rely on conventional source-root discovery by default.
- As a developer, I can override defaults with small TOML configuration.
- As a developer, I can rely on explicit configuration precedence.
- As a developer, I can receive structured diagnostics for invalid configuration.
- As a developer, I can rely on documented Python and PySpark support ranges.
- As a developer, I can configure `target_pyspark`.
- As a developer, I can configure `execution_mode`.
- As a developer, I can run `structure check`.
- As a developer, I can run compiler commands without PySpark, Java, SparkSession, Spark startup, or a Spark cluster.
- As a maintainer, I can review Sprint 00 spike notes before vertical slice coding begins.

### Sprint 01

- As a developer, I can define schema classes.
- As a developer, I can declare a transform class with `@transform`.
- As a developer, I can declare named inputs using `input(Structure)`.
- As a developer, I can run a transform online through `StructureSession`.
- As a developer, I can generate one PySpark class per transform class.
- As a developer, I can execute online or generated code against a Spark DataFrame.

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
- As a developer, diagnostic codes are registered with stable documentation links.
- As a developer, I receive structured compiler errors for unsupported Python.
- As a developer, I receive alternatives including DSL functions, `@expr_fn`, hooks, and config workarounds.

### Sprint 04

- As a developer, I can attach a hook with `@after(method)`.
- As a developer, I can write hook signature `def hook(self, *, df, spark, ctx)`.
- As a developer, online execution directly calls hooks when hooks exist.
- As a developer, generated code directly calls hooks when hooks exist.
- As a developer, hook-free generated code remains clean.

### Sprint 05

- As a developer, I can perform symbolic `join_one(...)` joins.
- As a developer, I can build serial joins across arbitrary numbers of named inputs.
- As a developer, I can inspect compiler provenance from source node to IR node to generated PySpark node.
- As a developer, I can inspect static dataflow lineage for transform, table, and column dependencies inferred from IR.
- As a developer, I can run `structure compile --fail-on-diff` in CI.
- As a developer, I can run compiler commands in ordinary Python CI without provisioning Spark.
- As a developer, online and generated transforms remain streaming-compatible when Spark supports the operations used.

### Sprint 07

- As a developer, I can use existence joins so that semi and anti filters stay compiler-visible.
- As a developer, I can use `join_many(...)` so that row multiplication is explicit.
- As a developer, I can use deterministic lookup dedupe policies so that selected right rows are reviewable.
- As a developer, I can use temporal validity-window lookups so that SCD-style joins have explicit interval semantics.
- As a developer, I can use backward as-of lookups so that time-relative enrichment stays compiler-visible.
- As a developer, I can inspect analytical join cardinality in static lineage and `structure explain`.
