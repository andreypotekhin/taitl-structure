# Traceability Matrix

This matrix maps early sprints to specification sections and major deliverables.

| Sprint | Spec Areas | Main Deliverables |
|---|---|---|
| Sprint 00 Groundwork | Setup, Configuration, Compatibility, Source Layout, Build Integration, Testing, Spikes | repo skeleton, CLI skeleton, config loader, compatibility policy, CI, source-root discovery, spike notes |
| Sprint 01 Vertical Slice 1 | Schemas, Transform Classes, Inputs, Online Execution, Generated Code, Testing | simple schema, one transform, online runner, generated class, Spark execution test |
| Sprint 02 Schemas and Validation | Schema Validation, Generated Code, Configuration | `StructType` generation, `assert_schema`, intermediate validation defaults |
| Sprint 03 Expressions/Filtering/Helpers | Symbolic Execution, Expression Helpers, Filtering, Error Reporting | expression IR, `where`, `@special(type="expr")`, diagnostic registry, structured unsupported-code errors |
| Sprint 04 Hooks/Generated Classes | Hooks, Generated Code, Error Reporting | `@raw(lane=lane)`, direct hook calls, no-hook cleanliness |
| Sprint 05 Joins/Compiler Traceability/Build | Joins, Compiler Traceability, Build Integration, Streaming Compatibility | `lookup_join`, N-step joins, compiler provenance, static dataflow traceability, `--fail-on-diff`, `explain` |
| Sprint 06 v2 Scope/Analytical IR | v2 Foundations, Backend Capabilities, Traceability, Streaming Compatibility | v2 scope, non-goals, operation taxonomy, capability placeholders, fixture skeletons, diagnostic anchors |
| Sprint 07 Analytical Join Coverage | Analytical Joins, Backend Capabilities, Traceability, Streaming Compatibility | existence joins, `inner_join`, deterministic lookup dedupe, temporal joins, as-of joins |
| Sprint 08 Aggregations/Windows/HOFs | Aggregations, Windowing, Deduplication, Higher-Order Functions, Testing | typed `group_by`, aggregate helpers, window helpers, deterministic dedupe, array/map helpers, parity tests |
| Sprint 09 Spark Connect/Optimization/Explain | Advanced Analytics, Spark Connect, Full PySpark Joins, Optimization Directives, Explain, Testing | full aggregation/window/HOF coverage, supported Spark Connect batch variant, right/full/cross rowset joins, non-equi/disjunctive predicates, cache/persist first-slice directives, compact explain, static streaming compatibility |
| Sprint 10 Docs/Testing | Generated Docs, Test Tooling | generated schema/transform docs, pytest helpers |
| Sprint 11 v3 DSL/SQL Function Parity | DSL, SQL Functions, Backend Capabilities, Testing | complete Column API helpers, SQL function helpers, generated examples, and parity tests |
| Sprint 12 v3 Join Parity Hardening | Joins, Backend Capabilities, Traceability, Streaming Compatibility | using-key joins, right/full diagnostics, cross safety, strategy directives, forward as-of joins |
| Sprint 13 v3 Aggregation Parity | Aggregations, Backend Capabilities, Traceability | grouping sets, `having(...)`, aggregate-output predicate diagnostics |
| Sprint 14 v3 Window Parity | Windows, Backend Capabilities, Streaming Compatibility | null ordering, normalized multiple order keys, aggregate windows |
| Sprint 15 v3 Collection Helper Parity | Higher-Order Functions, Arrays, Maps, Testing | collection size/membership, map-key membership, array construction/repeat/union/except, element lookup/concat |
| Sprint 16 v3 Streaming Transformation Hardening | Spark Structured Streaming, Generated Code, Integration Testing | watermarked enrichment, dedupe, aggregation, bounded stream-stream joins, caller-owned output-mode guidance, and file-stream evidence |
| Sprint 17 v4 Transformation Coverage | API coverage, DSL, capabilities, testing | checked PySpark transformation inventory, public catalog, status tests, and v4 fixture skeleton |

## Relevant Specification Items by Sprint

### Sprint 00

- As a developer, I can install Structure as a Python package.
- As a developer, I can rely on conventional source-root discovery by default.
- As a developer, I can override defaults with small TOML configuration.
- As a developer, I can rely on explicit configuration precedence.
- As a developer, I can receive structured diagnostics for invalid configuration.
- As a developer, I can rely on documented Python and PySpark support ranges.
- As a developer, I can configure `target_profile`.
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
- As a developer, I can use `@special(type="expr")` helpers.
- As a developer, diagnostic codes are registered with stable documentation links.
- As a developer, I receive structured compiler errors for unsupported Python.
- As a developer, I receive alternatives including DSL functions, `@special(type="expr")`, hooks, and config workarounds.

### Sprint 04

- As a developer, I can attach a hook with `@raw(lane=lane)`.
- As a developer, I can write a selected lane hook signature such as `def hook(self, *, orders, spark, ctx)`.
- As a developer, online execution directly calls hooks when hooks exist.
- As a developer, generated code directly calls hooks when hooks exist.
- As a developer, hook-free generated code remains clean.

### Sprint 05

- As a developer, I can perform symbolic `lookup_join(...)` joins.
- As a developer, I can build serial joins across arbitrary numbers of named inputs.
- As a developer, I can inspect compiler provenance from source node to IR node to generated PySpark node.
- As a developer, I can inspect static dataflow traceability for transform, table, and column dependencies inferred from IR.
- As a developer, I can run `structure compile --fail-on-diff` in CI.
- As a developer, I can run compiler commands in ordinary Python CI without provisioning Spark.
- As a developer, online and generated transforms remain streaming-compatible when Spark supports the operations used.

### Sprint 06

- As a developer, I can see a published v2 scope and non-goals.
- As a developer, I can receive backend capability diagnostics for every v2 operation.
- As a developer, I can inspect v2 operation cardinality in explain output.
- As a developer, I can rely on online and generated execution using the same v2 PySpark recipe layer.
- As a developer, I can keep caller-owned streaming orchestration in v2.

### Sprint 07

- As a developer, I can use existence joins so that semi and anti filters stay compiler-visible.
- As a developer, I can use `inner_join(...)` so that row multiplication is explicit.
- As a developer, I can use deterministic lookup dedupe policies so that selected right rows are reviewable.
- As a developer, I can use temporal validity-window lookups so that SCD-style joins have explicit interval semantics.
- As a developer, I can use backward as-of lookups so that time-relative enrichment stays compiler-visible.
- As a developer, I can inspect analytical join cardinality in static traceability and `structure explain`.

### Sprint 08

- As a developer, I can define typed aggregation step methods.
- As a developer, I can group by one or more typed fields.
- As a developer, I can calculate common aggregate metrics.
- As a developer, I can define window expressions for ranking, dedupe, latest-row selection, and rolling metrics.
- As a developer, I can define lag and lead expressions.
- As a developer, I can select latest or earliest rows with deterministic tie policy.
- As a developer, I can use higher-order function helpers for arrays and maps.

### Sprint 09

- As a developer, I can define advanced grouping patterns so that rollups, cubes, grouping sets, and multi-level
  summaries are supported when practical.
- As a developer, I can calculate Boolean, statistical, approximate, and collection aggregate metrics.
- As a developer, I can reuse named window specifications with explicit row and range frames.
- As a developer, I can define distribution, value, and aggregate window expressions.
- As a developer, I can use additional symbolic array and map higher-order helpers.
- As a developer, I can add caching and persistence hints at step boundaries.
- As a developer, I receive explicit deferrals for repartition, coalesce, checkpoint, and broader join strategy hints
  until their physical-plan contracts are specified.
- As a developer, I can express right, full, and cross rowset joins.
- As a developer, I can express non-equi and disjunctive join predicates when they remain compiler-visible.
- As a developer, I can generate richer static dataflow explain output.
- As a developer, I can run completed batch transforms online against Spark Connect.
- As a developer, I can run generated completed batch transforms against Spark Connect.
- As a developer, I receive diagnostics before Spark Connect runs classic-only internals.
- As a maintainer, I can verify Spark Connect support through CI or a documented manual script.

### Sprint 10

- As a developer, I can generate documentation artifacts for schemas and transforms.
- As a developer, I can use pytest helpers for compiler checks, freshness, snapshots, diagnostics, and parity.

### Sprint 11

- As a developer, I can use membership and range predicates in compiler-visible expressions.
- As a developer, I can use string predicates, collection indexing, struct field helpers, rich casts, and ordering
  modifiers without hooks.
- As a developer, I can use planned string, date/time, numeric, and predicate SQL function helpers.
- As a developer, unsupported raw SQL strings and raw PySpark expression escape hatches fail before runtime.

### Sprint 12

- As a developer, I can use PySpark-style using-key joins for one key and multiple keys.
- As a developer, I receive clear right/full join diagnostics that name nullable sides and invalid output fields.
- As a developer, I can request cross joins only with explicit Cartesian acknowledgement.
- As a developer, I can request supported join strategies and receive capability diagnostics for unsupported ones.
- As a developer, I can use forward as-of joins with deterministic tolerance behavior.

### Sprint 13

- As a developer, I can use explicit grouping sets for custom subtotal layouts.
- As a developer, I can filter aggregate result rows with `having(...)`.
- As a developer, diagnostics distinguish pre-aggregate `where(...)`, metric-local filters, and post-aggregate
  `having(...)`.

### Sprint 14

- As a developer, I can specify null ordering in window order keys.
- As a developer, I can use multiple order keys consistently across window helpers.
- As a developer, I can calculate aggregate metrics over reusable window specs.
- As a developer, raw PySpark `WindowSpec` remains rejected with a clear diagnostic.

### Sprint 15

- As a developer, I can calculate collection sizes and test array membership.
- As a developer, I can construct, repeat, union, and subtract arrays with type validation.
- As a developer, I can look up array and map elements, test map-key membership, and concatenate maps with documented
  nullability, out-of-range, and duplicate-key behavior.
- As a developer, row-expanding generator helpers remain explicitly deferred.

### Sprint 16

- As a developer, I can declare compiler-visible watermarks and use admitted stateful streaming transformations.
- As a developer, I can receive diagnostics when a streaming aggregation or dedupe lacks its required watermark.
- As a developer, I can keep sources, sinks, triggers, checkpoints, output modes, and query lifecycle in caller-owned
  Spark code.
- As a maintainer, I can verify admitted streaming transformations through file-stream integration evidence.

### Sprint 17

- As a developer, I can look up a relevant PySpark transformation API and see Structure support, a scheduled slice, or
  an explicit alternative so missing parity is never surprising.
- As a maintainer, I can verify that each in-scope PySpark transformation API has exactly one documented status.
