# Product Backlog

## Epic: Pre-Coding Spikes and Decisions

- SPIKE: Prove `@after(method)` works inside class bodies.
- SPIKE: Prove class-local `@expr_fn` helpers work without a `self` parameter.
- SPIKE: Prove source-order method discovery with stable line numbers.
- SPIKE: Prove source-root discovery and generated `structure_generated.<source package>` import paths.
- SPIKE: Prove `structure check` and `structure compile` can run without PySpark, Java, SparkSession, Spark startup,
  or a Spark cluster.
- SPIKE: Prove a minimal generated PySpark execution test with local Spark.
- Implement the documented schema declaration syntax from `SchemaDeclarationSyntax.spec.md`.
- Implement nullability and coercion rules from `NullabilityAndTypeCoercion.spec.md`.
- Decide and document generated-code ownership rules before CI integration.
- Implement compatibility, versioning, and license policy before open-source packaging.

## Epic: Project Foundation

- Create Python package skeleton.
- Add CLI entrypoint.
- Add TOML configuration loader.
- Add explicit config resolution order: CLI flags, `[tool.structure]` in `pyproject.toml`, `structure.toml`, defaults.
- Add config schema validation for unknown keys and invalid values.
- Add structured config diagnostics with allowed values for enum-like settings.
- Add seed config generator.
- Add CI test workflow.
- Add formatting/linting setup.
- Add generated directory conventions.

## Epic: Schema Model

- Implement `Structure` base class.
- Implement field definitions.
- Implement primitive scalar types.
- Implement explicit type objects: `String()`, `Float()`, `Double()`, `Decimal(...)`, `Array(...)`, `Map(...)`, and
  `Struct(...)`.
- Implement schema inheritance semantics from `SchemaInheritance.spec.md`.
- Implement nullable metadata.
- Implement Spark `StructType` emitter.
- Implement schema equality and compatibility checks.
- Implement generated schema modules.

## Epic: Transform Discovery

- Implement `@transform` decorator.
- Discover transform classes under configured source directory.
- Preserve class member source order.
- Detect `input(...)` declarations.
- Identify public schema-returning methods.
- Identify `@expr_fn` helpers.
- Identify `@before(method)` and `@after(method)` hooks.
- Report ambiguous public methods.

## Epic: Symbolic Execution

- Implement symbolic row proxies.
- Implement field reference expressions.
- Implement literal expressions.
- Implement unary/binary expression operators.
- Implement schema output construction capture.
- Implement `where(...)` predicate capture.
- Implement expression helper symbolic execution.
- Implement unsupported-operation diagnostics.

## Epic: IR and Checks

- Define `TransformPlan`.
- Define `StepPlan`.
- Define expression IR.
- Define plan operations: `Filter`, `Project`, `Join`, `HookCall`, `ValidateSchema`.
- Validate source-order schema flow.
- Validate expression type compatibility.
- Validate `where(...)` predicates are boolean.
- Validate hook signatures.
- Validate performance guardrails.

## Epic: PySpark Code Generation

- Generate transform classes.
- Generate convenience functions.
- Generate schema modules.
- Generate `select(...)` projections.
- Generate `where(...)` filters.
- Generate `join(...)` operations.
- Generate intermediate validation.
- Generate hook calls only when needed.
- Generate formatted deterministic code.

## Epic: Runtime Support

- Implement `assert_schema(...)`.
- Implement `project_schema(...)`.
- Implement `PipelineContext`.
- Implement schema comparison utilities.
- Add runtime tests independent of compiler internals.

## Epic: Hooks

- Implement `@before(method)` metadata.
- Implement `@after(method)` metadata.
- Validate signature `def hook(self, *, df, spark, ctx)`.
- Generate direct source hook calls.
- Support schema mode and project-output options.
- Ensure no hook machinery in hook-free generated code.

## Epic: Joins

- Implement symbolic named input scopes.
- Implement `join_one(...)`.
- Implement join type enum.
- Implement join hint enum.
- Generate aliases predictably.
- Support arbitrary N-step serial joins.
- Warn when `join_one(...)` lacks uniqueness metadata.

## Epic: Compiler Lineage

- Add compiler provenance from source node to IR node to generated PySpark node.
- Add static dataflow lineage inferred from IR.
- Track transform, named input, step, schema, field, join, filter, expression helper, and hook-boundary dependencies.
- Surface provenance and static dataflow in compiler diagnostics.
- Add `structure explain` lineage output for transform, step, and field dependencies.
- Add streaming compatibility reporting with compatible, batch-only, and unknown states.
- Add diagnostic codes with documentation links.
- Add `structure doctor` or equivalent setup/configuration checks.

## Epic: Build Integration

- Implement `structure check`.
- Implement `structure compile`.
- Implement `structure explain`.
- Implement `structure compile --fail-on-diff`.
- Add no-Spark guard tests for compiler commands.

## v2 Backlog

- Windowing.
- Deduplication helpers.
- Aggregations.
- Advanced aggregation/grouping.
- Spark higher-order functions.
- Caching/persistence directives.
- Repartition/coalesce hints.
- Advanced join strategy directives.
- `join_many(...)` and other row-multiplying or existence-oriented join forms.
- Richer static dataflow explain output.
- Production incremental compile and cache diagnostics.
- Generated documentation artifacts for schemas and transforms.
- Pytest helper or plugin for compiler checks, generated-code freshness, and generated-code snapshots.

## v3 Backlog

- Streaming source declarations.
- Streaming sink declarations.
- Generated `readStream`.
- Generated `writeStream`.
- Triggers.
- Checkpoints.
- Output modes.
- Watermarks.
- Stateful streaming policies.

## v4 Backlog

- Spark Connect support.
- Spark Connect compatibility tests.
- Backend capability reporting for ordinary PySpark and Spark Connect targets.

## Nice To Have Beyond v4

- Runtime LDJSON lineage emitter. See `docs/dev/project-management/NiceToHave.md`.
