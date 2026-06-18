# Structure Specification

This document is a user-story specification for SDLC planning. Early sections cover general setup and getting started. Later sections cover narrower use cases and roadmap features.

## 1. Setup

- As a developer, I can install Structure as a Python package so that I can define schema-driven Spark transformations in Python.
- As a developer, I can use ordinary Python source roots such as `src` or the project root so that Structure fits common package layouts.
- As a developer, I can generate code under `generated/structure_generated` so that generated imports avoid source package collisions.
- As a developer, I can configure alternate source and generated directories so that Structure fits existing project layouts.
- As a developer, I can run `structure init --seed-config` so that all default settings are visible.
- As a developer, I can omit configuration entirely so that seed defaults are used.

## 2. Configuration

- As a developer, I can configure Structure through `[tool.structure]` in `pyproject.toml` so that settings live with my Python project.
- As a developer, I can configure Structure through `structure.toml` so that non-`pyproject` layouts are supported.
- As a developer, I can override configuration with CLI flags so that CI and local commands can vary behavior.
- As a developer, I can rely on explicit config resolution order of CLI flags, `[tool.structure]` in `pyproject.toml`,
  `structure.toml`, then defaults.
- As a developer, I can receive structured diagnostics for unknown config keys and invalid values so that configuration mistakes are easy to fix.
- As a developer, I can see allowed values for enum-like config settings so that values such as lineage levels are corrected quickly.
- As a developer, I can set `source_roots` so that transform source files are discovered predictably.
- As a developer, I can set `generated_dir` so that generated files are written predictably.
- As a developer, I can set `target_pyspark` so that generated code targets an intended PySpark version range.
- As a developer, I can receive an error when a requested feature cannot be generated for the configured PySpark target.
- As a developer, I can set validation defaults so that schema enforcement is project-wide and repeatable.
- As a developer, I can set lineage level so that output detail is controlled.
- As a developer, I can set `fail_on_diff` so that CI catches stale generated code.

## 3. Getting Started

- As a developer, I can define schema classes so that input, intermediate, and output data structures are explicit.
- As a developer, I can declare a transform class with `@transform` so that the compiler knows which classes to generate.
- As a developer, I can declare named inputs using `input(Schema)` so that generated `run(...)` methods receive predictable named DataFrame parameters.
- As a developer, I can write public schema-returning methods so that each method becomes a compiled subtransform.
- As a developer, I can run `structure check` so that compileability issues are caught without writing generated files.
- As a developer, I can run `structure compile` so that source transforms produce generated PySpark classes.

## 4. Source Layout

- As a developer, I can place schemas under my normal source package so that schema definitions are easy to locate.
- As a developer, I can place transforms under my normal source package so that transformation logic is easy to locate.
- As a developer, I can omit source-root configuration when `./src` contains importable packages so that conventional projects work with no setup.
- As a developer, I can omit source-root configuration in simple root-package projects so that small projects work with no setup.
- As a developer, I can rely on source file order inside transform classes so that subtransform execution order matches code reading order.
- As a developer, I can avoid external configuration files for ordinary transform discovery so that project setup remains simple.
- As a developer, I can use Python imports for schema and helper references so that IDE jump-to-declaration works.

## 5. Schemas

- As a developer, I can define fields with types and nullability so that generated Spark schemas are explicit.
- As a developer, I can declare fields with explicit type objects such as `String()` and `Decimal(12, 2)` so that
  schema syntax is unambiguous and extensible.
- As a developer, I can declare array, map, and nested struct fields so that semi-structured Spark data remains typed.
- As a developer, I can inherit fields from other schema classes so that shared field groups do not need to be
  duplicated.
- As a developer, I can define primary keys or uniqueness hints so that join cardinality warnings are possible.
- As a developer, I can define intermediate schemas so that multi-step transformations are validated between steps.
- As a developer, I can generate Spark `StructType` declarations so that reads and validations use consistent schemas.

## 6. Transform Classes

- As a developer, I can decorate a class with `@transform` so that it becomes a Structure transform.
- As a developer, I can define one source transform class per logical transformation pipeline so that generated code maps cleanly to source code.
- As a developer, I can generate one PySpark class per source transform class so that generated code remains organized.
- As a developer, I can instantiate a generated transform class with `spark` and optional `ctx` so that runtime dependencies are explicit.
- As a developer, I can call `run(...)` on a generated transform class so that execution has a stable entrypoint.

## 7. Inputs

- As a developer, I can declare an input with `orders = input(OrderRaw)` so that the compiler knows the expected input schema.
- As a developer, I can declare multiple named inputs so that generated `run(...)` uses named keyword arguments.
- As a developer, I can refer to input scopes symbolically inside joins so that joins avoid string field paths.
- As a developer, I can validate input DataFrames against declared schemas so that schema errors are caught at pipeline boundaries.

## 8. Subtransforms

- As a developer, I can define a public instance method returning a schema type so that it becomes a compiled subtransform.
- As a developer, I can use method return annotations to define intermediate schema transitions.
- As a developer, I can rely on source order for subtransform execution so that pipeline flow is readable.
- As a developer, I can chain subtransforms by return type and next input type so that schema flow is validated.
- As a developer, I can receive a structured compiler error when source order does not match type flow.
- As a developer, I can make ordinary helper methods private with a leading underscore so that they are not treated as subtransforms.

## 9. Schema Validation

- As a developer, I can have input schemas validated at runtime so that invalid source data is detected early.
- As a developer, I can have intermediate schemas validated after each subtransform by default so that multi-step
  pipelines remain schema-safe.
- As a developer, I can use schema-only intermediate validation by default so that validation avoids unnecessary row scans.
- As a developer, I can opt into fuller intermediate validation so that row-level constraints can be checked when needed.
- As a developer, I can disable intermediate validation class-wide so that performance-sensitive pipelines can reduce
  validation overhead.
- As a developer, I can override validation for an individual subtransform so that known temporary exceptions are possible.
- As a developer, I can disable intermediate schema validation project-wide so that large pipelines can remove the
  generated boundary checks.
- As a developer, I can have final output schema validation enabled by default so that generated outputs conform to their declared contract.

## 10. Generated Code

- As a developer, I can inspect generated PySpark code so that transformation behavior is reviewable.
- As a developer, I can generate a class named after the source transform class so that source-to-generated mapping is obvious.
- As a developer, I can expect generated code to use PySpark DataFrame and Column operations so that Spark can optimize execution.
- As a developer, I can expect generated code to omit hook imports when no hooks are defined so that generated code stays clean.
- As a developer, I can expect generated code to contain stable variable names such as `df`, `spark`, and `ctx` so that generated code is predictable.

## 11. Symbolic Execution

- As a developer, I can write compiled subtransforms using schema objects so that the compiler can symbolically execute transformation logic.
- As a developer, I can have field access produce symbolic expressions so that field references compile to Spark columns.
- As a developer, I can have DSL functions produce symbolic expressions so that transforms compile to Spark expressions.
- As a developer, I can have unsupported Python operations rejected so that hidden UDF-like behavior is avoided.

## 12. Expression Helpers

- As a developer, I can define module-level `@expr_fn` helpers so that expression logic is reusable across transforms.
- As a developer, I can define class-local `@expr_fn` helpers so that expression logic stays near the transform using it.
- As a developer, I can define class-local expression helpers without a `self` parameter so that they behave like pure expression functions.
- As a developer, I can call class-local expression helpers through `self` so that the call site remains discoverable by IDEs.

## 13. Filtering

- As a developer, I can call `where(predicate)` inside a subtransform so that rows are filtered using compileable Spark expressions.
- As a developer, I can call `where(...)` multiple times so that predicates are combined with logical AND.
- As a developer, I can use expression helper predicates with `where(...)` so that reusable filters are supported.
- As a developer, I can filter on joined fields so that post-join match requirements are expressible.

## 14. Add and Drop Columns

- As a developer, I can add columns by returning an output schema with more fields than the input schema.
- As a developer, I can drop columns by returning an output schema with fewer fields than the input schema.
- As a developer, I can rely on generated projection rather than Spark `drop(...)` so that output schema is deterministic.
- As a developer, I can remove temporary intermediate fields in a later subtransform so that final output remains clean.

## 15. Joins

- As a developer, I can declare multiple named inputs so that join sources are explicit.
- As a developer, I can express joins symbolically using input scopes so that join logic avoids string column paths.
- As a developer, I can use `join_one(...)` for many-to-one or one-to-one lookup joins so that cardinality intent is explicit.
- As a developer, I can use `join_many(...)` for cardinality-expanding joins so that row multiplication is explicit.
- As a developer, I can perform serial joins across an arbitrary number of inputs so that enrichment pipelines are not limited to three inputs.
- As a developer, I can specify join type and hints using enum values so that free-form join strings are avoided in source code.

## 16. Hooks

- As a developer, I can attach a hook to a subtransform using `@before(method)` or `@after(method)` so that custom PySpark code is tied to a concrete method.
- As a developer, I can write hook methods with signature `def hook(self, *, df, spark, ctx)` so that hook parameters are minimal and stable.
- As a developer, I can opt a hook into original input access with `pass_inputs=True` so that unusual validation or
  lookup logic can use named source DataFrames.
- As a developer, I can use arbitrary PySpark DataFrame code inside hooks so that escape hatches are available.
- As a developer, I can expect generated code to call hooks directly on the source transform instance so that hook behavior is transparent.

## 17. Streaming Compatibility

- As a developer, I can pass a streaming DataFrame to generated transforms when operations are Spark streaming-compatible.
- As a developer, I can enable streaming compatibility checks so that unsupported streaming operations are caught early.
- As a developer, I can keep streaming orchestration outside Structure in v1 and v2 so that callers own `readStream`, `writeStream`, triggers, and checkpoints.

## 17A. Versioning and Compatibility

- As a developer, I can rely on a documented Python support range so that runtime expectations are clear.
- As a developer, I can rely on a documented PySpark support range so that generated code uses compatible APIs.
- As a developer, I can configure `target_pyspark` so that the emitter avoids APIs outside my deployment range.
- As a developer, I can see that Spark Connect is planned for v3 so that v1/v2 generated-code expectations are clear.
- As a developer, I can rely on semantic versioning after 1.0 so that upgrades carry predictable risk.
- As a developer, I can rely on versioned lineage LDJSON so that downstream lineage consumers can evolve safely.
- As a developer, I can rely on config schema compatibility rules so that project configuration changes are intentional.

## 18. Lineage

- As a developer, I can generate per-transform LDJSON lineage so that each transform line shows inputs, output, steps,
  joins, and hooks.
- As a developer, I can optionally generate field-level LDJSON lineage so that output fields can be traced to source fields when needed.
- As a developer, I can identify opaque hook boundaries so that arbitrary PySpark logic is visible in lineage reports.
- As a developer, I can keep lineage output compact by default so that generated artifacts remain manageable.

## 19. Build Integration

- As a developer, I can run `structure check` in CI so that compile errors are caught before tests.
- As a developer, I can run `structure compile --fail-on-diff` in CI so that generated code is kept in sync.
- As a developer, I can run compiler commands without PySpark, Java, SparkSession, Spark startup, or a Spark cluster so
  that ordinary Python CI can validate source quickly.
- As a developer, I can run Structure as part of Python project build scripts so that generation fits normal development workflows.
- As a developer, I can use seed config defaults so that project configuration remains small.

## 20. Error Reporting

- As a developer, I can receive structured compiler errors so that failures are actionable.
- As a developer, I can see the transform class name, subtransform method, output field, source expression, problem, and suggested fix in errors.
- As a developer, I can see an inline DSL alternative so that simple fixes are obvious.
- As a developer, I can see an `@expr_fn` helper alternative so that reusable fixes are encouraged.
- As a developer, I can see a hook alternative so that arbitrary PySpark migration is explicit.
- As a developer, I can see a configuration workaround when a safe config setting exists.

## 21. Testing

- As a developer, I can compile transforms during tests so that unsupported code is caught before deployment.
- As a developer, I can test generated PySpark code against small Spark DataFrames so that transformation behavior is verified.
- As a developer, I can snapshot generated code so that generator changes are reviewable.
- As a developer, I can assert schema validation behavior so that invalid inputs fail as expected.
- As a developer, I can assert configuration validation behavior so that invalid settings fail with structured diagnostics.
- As a developer, I can run intentionally broken transform tests so that compiler diagnostics stay actionable.
- As a developer, I can assert warning diagnostics so that risky but compileable code remains visible in tests.

## 22. v2 Roadmap

- As a developer, I can define typed aggregation subtransforms so that rollups compile to Spark `groupBy` and `agg`.
- As a developer, I can define advanced grouping patterns so that rollups, cubes, grouping sets, and multi-level summaries are supported.
- As a developer, I can define window expressions so that ranking, deduplication, latest-record selection, and rolling metrics compile to Spark window operations.
- As a developer, I can use higher-order function helpers so that array and map transformations remain Spark-plan-visible.
- As a developer, I can add caching and persistence hints at step boundaries so that expensive reused DataFrames can be optimized explicitly.
- As a developer, I can specify join strategies and hints so that manual optimization remains explicit and reviewable.
- As a developer, I can generate optional field-level lineage so that output fields can be traced to source fields when needed.

## 23. v3 Roadmap

- As a developer, I can target Spark Connect when Structure defines and tests a compatible generated-code contract.
- As a developer, I can define streaming sources so that Structure can generate `readStream` code.
- As a developer, I can define streaming sinks so that Structure can generate `writeStream` code.
- As a developer, I can define triggers and checkpoint locations so that streaming jobs are deployable from generated code.
- As a developer, I can define watermarks and state policies so that full streaming orchestration is explicit.
