# Sprint 02: Schemas and Validation

## Sprint Goal

Turn the vertical slice into a schema-enforced pipeline with generated Spark `StructType` modules and runtime input/intermediate/output validation.

## Product Outcome

Developers can rely on declared schemas to catch invalid DataFrame structure at runtime, and intermediate schemas are validated by default.

## Scope

### In Scope

- Spark `StructType` generation.
- Runtime `assert_schema(...)`.
- Runtime `project_schema(...)` if needed for output projection.
- Input validation.
- Output validation.
- Intermediate validation for multi-step transforms.
- Class-wide validation config.
- Method-level validation override.
- Config fallback hints in validation errors.

### Out of Scope

- Complex nested schemas beyond a basic nested test.
- Field-level lineage.
- Joins.
- Hooks.

## Relevant Specification Items

- As a developer, I can generate Spark `StructType` schemas.
- As a developer, I can validate input schemas at runtime.
- As a developer, I can validate intermediate schemas after each subtransform by default.
- As a developer, I can validate final output schemas.
- As a developer, I can disable intermediate validation class-wide.
- As a developer, I can override validation for an individual subtransform.
- As a developer, validation errors can suggest relevant config settings when applicable.

## Deliverables

- `generated/schemas/*.py` modules.
- `generated/runtime/schema_assert.py`.
- Validation policy model.
- Config-driven validation defaults.
- Tests for passing and failing schema validation.

## Engineering Tasks

1. Implement `SchemaDef` and `FieldDef` IR refinements.
2. Generate Spark `StructType` for primitive types.
3. Implement schema comparison logic.
4. Implement `assert_schema(...)`.
5. Implement validation policy resolution.
6. Generate input validation calls.
7. Generate intermediate validation calls.
8. Generate final validation calls.
9. Add class-wide `validate_intermediate` override.
10. Add method-level `@validate_output(False)`.
11. Add validation error messages with config workaround hints.

## Acceptance Criteria

- Generated schema modules import successfully.
- Invalid input schema fails with useful error.
- Invalid intermediate schema fails when validation enabled.
- Disabling intermediate validation removes intermediate validation calls.
- Final output validation remains enabled by default.

## Demo Script

```bash
structure compile --src tests/fixtures/schema_validation/structure/src --out /tmp/structure/generated
pytest tests/test_schema_validation.py
```

## Compile-Time Performance Metric

Track compile time for a fixture with 5 schemas and 3 transforms.

Target:

- Under 2 seconds excluding Spark startup.

## Risks

- Runtime validation could become too strict or too slow.
- Spark type compatibility rules can be nuanced.

## Notes

Validation should compare schema structure, not scan rows. It should be cheap relative to Spark job execution.
