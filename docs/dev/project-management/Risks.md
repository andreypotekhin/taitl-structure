# Risks and Mitigations

## Risk: Symbolic execution becomes too magical

### Impact

Developers may be confused when normal Python code is rejected.

### Mitigation

- Keep DSL functions explicit.
- Provide structured errors.
- Suggest direct DSL replacement, `@expr_fn`, hook, and config workaround where relevant.
- Document performance rationale clearly.

## Risk: Generated code becomes bloated

### Impact

Generated PySpark becomes hard to review and less attractive to users.

### Mitigation

- Generate classes with concise sections.
- Omit hook machinery when hooks are absent.
- Use deterministic aliases.
- Keep default compiler lineage compact.
- Add snapshot tests for generated code readability.

## Risk: Compile time becomes slow

### Impact

Users avoid running compiler frequently, causing stale generated code.

### Mitigation

- Track compile-time metrics starting in early sprints.
- Cache discovery results where safe.
- Avoid repeated module imports.
- Avoid expensive AST/CST parsing unless diagnostics need it.
- Add performance fixtures for 10-transform and 100-transform projects.

## Risk: PySpark API changes break generated code

### Impact

Generated code may become incompatible with newer Spark versions.

### Mitigation

- Isolate PySpark calls in emitter layer.
- Maintain PySpark capability registry.
- Run multi-version CI.
- Snapshot generated code per target version where necessary.

## Risk: Spark Connect support distorts v1/v2 scope

### Impact

Early Spark Connect work could force generated class API changes before the ordinary PySpark contract is stable.

### Mitigation

- Schedule Spark Connect for v3.
- Allow earlier work only if it stays inside the existing PySpark emitter boundary.
- Require compatibility tests before public support is documented.
- Keep public v1/v2 docs explicit that generated code targets ordinary PySpark APIs.

## Risk: Hooks compromise performance

### Impact

Users may put inefficient PySpark or local Python operations into hooks.

### Mitigation

- Keep compiled path strict.
- Add hook linting in strict-performance mode.
- Document hooks as explicit escape hatches.
- Show hooks as opaque in lineage.

## Risk: Decorator mechanics fail after design is committed

### Impact

Class-body hook declarations or class-local expression helpers may require awkward syntax changes if Python descriptor and namespace behavior is not proven early.

### Mitigation

- Spike `@after(method)` binding inside class bodies in Sprint 00.
- Spike class-local `@expr_fn` helpers callable through `self` without a `self` parameter.
- Capture source locations and source order in the same spike notes.

## Risk: Compiler accidentally depends on Spark during checks or compile

### Impact

`structure check` or `structure compile` becomes slow and hard to run in CI if it imports PySpark, starts Spark,
requires Java, needs a SparkSession, or contacts a Spark cluster.

### Mitigation

- Treat no-Spark check and compile as a Sprint 00 proof.
- Keep Spark execution tests separate from compiler checks.
- Add guard tests that fail if compiler commands import PySpark on the no-Spark path.

## Risk: Intermediate validation overhead is too high

### Impact

Runtime performance may suffer on very large pipelines.

### Mitigation

- Enable validation by default for safety.
- Default intermediate validation to schema-only checks.
- Allow explicit opt-in to schema-and-constraint validation.
- Allow class-wide and method-level overrides.
- Allow project-wide `validate_intermediate = false` for externally validated or performance-sensitive
  pipelines.
- Suggest config workaround in validation-related diagnostics.
- Make validation implementation efficient and schema-only, not row-scanning.
