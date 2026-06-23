# Sprint 02: Schemas and Validation

## Sprint Goal

Turn the vertical slice into a schema-enforced pipeline with Spark `StructType` modules, generated schemas usable by
caller code, online-materialized schemas after `.run(session)`, and online/generated runtime
input/intermediate/output validation.

## Product Outcome

Developers can rely on declared schemas to catch invalid DataFrame structure at runtime. Intermediate schemas are
validated by default, and callers can reuse generated or online-materialized schema constants for reads and pre-write
validation/projection.

## Scope

### In Scope

- Spark `StructType` generation.
- Runtime `assert_schema(...)`.
- Runtime `project_schema(...)` if needed for output projection.
- Generated schema constants usable by caller code for `spark.read.schema(...)` and pre-write validation/projection.
- Online-materialized output schema available after `.run(session)`.
- Primitive, array, map, and nested struct schema generation.
- Input validation.
- Output validation.
- Intermediate validation for multi-step transforms.
- Class-wide validation config.
- Method-level validation override.
- Config fallback hints in validation errors.

### Out of Scope

- Deep recursive schema graphs.
- Data-quality constraint execution beyond schema shape.
- Compiler traceability details.
- Joins.
- Hooks.

## Relevant Specification Items

- As a developer, I can generate Spark `StructType` schemas.
- As a developer, I can import generated schema constants in caller code.
- As a developer, I can access the output Spark schema after online execution.
- As a developer, I can validate input schemas at runtime.
- As a developer, I can validate intermediate schemas after each subtransform by default.
- As a developer, I can validate final output schemas.
- As a developer, I can disable intermediate validation class-wide.
- As a developer, I can override validation for an individual subtransform.
- As a developer, validation errors can suggest relevant config settings when applicable.
- As a developer, online and generated execution use the same schema validation policy.

## Deliverables

- schema modules usable by online and generated execution.
- schema modules usable by caller-owned reads and writes.
- shared runtime `schema_assert.py`.
- Validation policy model.
- Input, intermediate, and output validation modes.
- Config-driven validation defaults.
- Tests for passing and failing schema validation.

## Engineering Tasks

1. Implement `SchemaDef` and `FieldDef` IR refinements.
2. Generate Spark `StructType` for primitive, array, map, and nested struct types.
3. Implement schema comparison logic.
4. Implement `assert_schema(...)`.
5. Implement validation policy resolution.
6. Document and test generated schema reuse from caller code.
7. Materialize online output schemas from the same schema model.
8. Add `input_validation_mode`, `intermediate_validation_mode`, and `output_validation_mode`.
9. Generate input validation calls.
10. Generate intermediate validation calls.
11. Generate final validation calls.
12. Add class-wide `validate_intermediate` override.
13. Add method-level `@validate_output(False)`.
14. Add validation error messages with config workaround hints.

## Acceptance Criteria

- Generated schema modules import successfully.
- Generated schemas cover primitive, array, map, and nested struct fields.
- Generated schema constants work with caller-owned reads and pre-write validation/projection.
- Online execution exposes an equivalent output Spark schema after `.run(session)`.
- Online execution validates schemas through the same runtime helpers.
- Invalid input schema fails with useful error.
- Invalid intermediate schema fails when validation enabled.
- Disabling intermediate validation removes intermediate validation calls.
- Final output validation remains enabled by default.

## Progress

- [x] (2026-06-21) Spark schema source rendering, generated schema modules, runtime schema helper rendering, and runtime
  schema materialization cover primitive, decimal, array, map, nested struct, and inherited schemas.
- [x] (2026-06-21) Validation recipe placement is represented in shared PySpark execution plans for input,
  intermediate, hook, projection, and final output validation boundaries.
- [x] (2026-06-21) Online execution exposes materialized input, step, and output schemas through
  `transform.schemas` after `run(session)`.
- [x] (2026-06-23) Live `assert_schema(...)` execution is exercised by generated and online runtime paths in the
  PySpark integration matrix.
- [ ] v1 closeout: add broader negative schema-validation coverage against Spark DataFrames.

## Demo Script

```bash
structure compile --source-root tests/fixtures/schema_validation/src --out /tmp/generated
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
