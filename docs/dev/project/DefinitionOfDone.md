# Definition of Done

A story or task is done when all relevant criteria are satisfied.

An implementation task may finish before sprint integration; support claims and completed user stories require
their specified positive runtime evidence. Sprint completion also requires the full configured integration matrix.
Follow the [validation cadence](../Testing.md#validation-cadence).

## Functional Criteria

- The requested behavior is implemented.
- Source DSL examples compile.
- Generated PySpark code imports successfully.
- Runtime behavior is covered by tests where applicable.
- Error cases are covered by tests where applicable.

## Generated Code Criteria

- Generated code is committed with the source or configuration changes that produced it.
- Generated code records Structure generator version and configured target where required.
- Generated code is not edited manually; changes come from Structure source, configuration, or generator updates.
- Generated code is deterministic.
- Generated code is formatted.
- Generated code is readable enough for review.
- Generated code uses Spark DataFrame/Column APIs for compiled paths.
- Generated code does not use UDFs, `rdd`, `collect`, `toPandas`, or row-wise Python maps for compiled step methods.
- Hook-free generated code contains no source transform import and no hook machinery.

## Compiler Criteria

- `structure check`, `structure compile`, and generated-file diff checks run without PySpark, Java, SparkSession, or
  Spark startup.
- Compilation time is measured for relevant fixtures.
- Compile-time regressions are avoided.
- Unsupported Python operations fail with structured errors.
- Error messages include specific location/context where feasible.
- Error messages suggest direct DSL alternatives, `@special(type="expr")` helpers, hooks, and config workarounds when applicable.

## Schema Criteria

- Input schemas are validated when enabled.
- Intermediate schemas are validated by default.
- Output schemas are validated.
- Validation override behavior is covered by tests.

## Documentation Criteria

- Public behavior is documented in user docs.
- Compatibility behavior is documented in public docs when a change affects Python, PySpark, generated code, traceability, or
  config schema.
- Internal behavior is documented in dev docs when design-relevant.
- Specification/user-story references are updated.
- Examples show both source Structure code and generated PySpark when clarity benefits.

## Testing Criteria

- `make build` passes after each coding task; documentation-only tasks use relevant consistency and link checks.
- Unit tests pass.
- Compiler tests pass.
- `structure compile --fail-on-diff` passes when generated artifacts are in scope.
- Generated code snapshot tests pass.
- Generated code import tests pass.
- Focused live integration checks are chosen as needed during tasks; record any checks performed and their scope.
- Required PySpark execution and parity evidence passes before admitting support or marking user stories complete.
- Negative compiler and diagnostic tests cover relevant broken transforms and warnings.
- CI scripts are updated if needed.

## Sprint Acceptance Criteria

- `make build INTEGRATION=1 BACKEND=all` passes against the sprint's closing candidate, including every configured
  backend selection and required isolated provider phase.
- Integration regressions are resolved; affected selections are rerun after fixes, with broader reruns when shared
  changes invalidate other results.
- The sprint review or ExecPlan records the tested revision, commands, versions, selection scope, passed/failed/skipped
  totals, reasons, outstanding gates, and log locations. Missing or skipped evidence is not a pass.
- Version hardening retains its additional compatibility, performance, documentation, and migration checks.

## Review Criteria

- Public API naming is reviewed.
- Generated code shape is reviewed.
- Performance implications are reviewed.
- Backwards compatibility concerns are noted.
