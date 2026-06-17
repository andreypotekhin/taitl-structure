# Definition of Done

A story or task is done when all relevant criteria are satisfied.

## Functional Criteria

- The requested behavior is implemented.
- Source DSL examples compile.
- Generated PySpark code imports successfully.
- Runtime behavior is covered by tests where applicable.
- Error cases are covered by tests where applicable.

## Generated Code Criteria

- Generated code is deterministic.
- Generated code is formatted.
- Generated code is readable enough for review.
- Generated code uses Spark DataFrame/Column APIs for compiled paths.
- Generated code does not use UDFs, `rdd`, `collect`, `toPandas`, or row-wise Python maps for compiled subtransforms.
- Hook-free generated code contains no source transform import and no hook machinery.

## Compiler Criteria

- Compilation time is measured for relevant fixtures.
- Compile-time regressions are avoided.
- Unsupported Python operations fail with structured errors.
- Error messages include specific location/context where feasible.
- Error messages suggest direct DSL alternatives, `@expr_fn` helpers, hooks, and config workarounds when applicable.

## Schema Criteria

- Input schemas are validated when enabled.
- Intermediate schemas are validated by default.
- Output schemas are validated.
- Validation override behavior is covered by tests.

## Documentation Criteria

- Public behavior is documented in user docs.
- Internal behavior is documented in dev docs when design-relevant.
- Specification/user-story references are updated.
- Examples show both source Structure code and generated PySpark when clarity benefits.

## Testing Criteria

- Unit tests pass.
- Compiler tests pass.
- Generated code snapshot tests pass.
- Generated code import tests pass.
- PySpark execution tests pass where applicable.
- CI scripts are updated if needed.

## Review Criteria

- Public API naming is reviewed.
- Generated code shape is reviewed.
- Performance implications are reviewed.
- Backwards compatibility concerns are noted.
