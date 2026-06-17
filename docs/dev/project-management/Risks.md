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
- Keep lineage basic by default.
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

## Risk: Hooks compromise performance

### Impact

Users may put inefficient PySpark or local Python operations into hooks.

### Mitigation

- Keep compiled path strict.
- Add hook linting in strict-performance mode.
- Document hooks as explicit escape hatches.
- Show hooks as opaque in lineage.

## Risk: Default `structure/src` layout affects IDE tooling

### Impact

IDEs may not automatically treat `structure/src` as a source root.

### Mitigation

- Document marking `structure/src` as source root.
- Keep paths configurable.
- Avoid making `structure/` itself an import package unless intentionally configured.
- Generate importable package paths predictably under `structure/generated`.

## Risk: Intermediate validation overhead is too high

### Impact

Runtime performance may suffer on very large pipelines.

### Mitigation

- Enable validation by default for safety.
- Allow class-wide and method-level overrides.
- Suggest config workaround in validation-related diagnostics.
- Make validation implementation efficient and schema-only, not row-scanning.
