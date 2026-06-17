# Product Backlog

## Epic: Project Foundation

- Create Python package skeleton.
- Add CLI entrypoint.
- Add TOML configuration loader.
- Add seed config generator.
- Add CI test workflow.
- Add formatting/linting setup.
- Add generated directory conventions.

## Epic: Schema Model

- Implement `Schema` base class.
- Implement field definitions.
- Implement primitive scalar types.
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

## Epic: Lineage

- Emit basic LDJSON lineage.
- Include transform, input, step, join, hook, and output events.
- Add lineage level config: `none`, `basic`, `fields`, `debug`.
- Keep `basic` compact by default.

## Epic: Build Integration

- Implement `structure check`.
- Implement `structure compile`.
- Implement `structure explain`.
- Implement `structure compile --fail-on-diff`.
- Add pytest helper or plugin later.

## v2 Backlog

- Aggregations.
- Advanced aggregation/grouping.
- Windowing.
- Deduplication helpers.
- Spark higher-order functions.
- Caching/persistence directives.
- Repartition/coalesce hints.
- Advanced join strategy directives.
- Field-level lineage.

## v3 Backlog

- Streaming source declarations.
- Streaming sink declarations.
- Generated `readStream`.
- Generated `writeStream`.
- Triggers.
- Checkpoints.
- Watermarks.
- Stateful streaming policies.
