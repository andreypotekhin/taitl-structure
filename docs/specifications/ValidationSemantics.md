# Validation Semantics

## Purpose

Validation is how Structure proves that live DataFrames conform to declared schemas at pipeline boundaries. It must be
strong enough to catch drift and weak enough by default to avoid hidden Spark work.

This specification owns validation phases, validation modes, strictness, hook integration, output projection,
configuration precedence, runtime behavior, and acceptance tests. Data-quality constraint families are specified in
`docs/specifications/DataQualityConstraints.md`.

## Validation Phases

Structure has three runtime validation phases:

```text
input
intermediate
output
```

Input validation checks DataFrames supplied to the transform invocation.

Intermediate validation checks the DataFrame after each compiled subtransform and its attached hooks, according to
project, class, and method policy.

Output validation checks the final returned DataFrame.

## Validation Modes

Allowed validation mode values:

```text
off
schema_only
schema_and_constraints
```

Mode meanings:

- `off`: do not validate this phase.
- `schema_only`: compare DataFrame schema shape without scanning rows.
- `schema_and_constraints`: run schema validation and any constraints eligible for the phase.

`schema_and_constraints` is allowed as a configuration value, but until concrete constraints are implemented it may
produce a diagnostic explaining that only schema checks are currently available.

## Default Policy

Default configuration:

```toml
[tool.structure]
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
```

Rules:

- `validate_intermediate = false` is a compatibility shortcut for `intermediate_validation_mode = "off"`.
- If both are set, `intermediate_validation_mode` is authoritative and `validate_intermediate` must agree or produce a
  configuration diagnostic.
- The default must not trigger row scans.
- Output validation remains enabled by default.

## Policy Precedence

Validation policy is resolved from broadest to narrowest:

1. Built-in defaults.
2. `structure.toml`.
3. `[tool.structure]` in `pyproject.toml`.
4. CLI flags.
5. `@transform(validate_intermediate=...)` class-level override.
6. `@validate_output(...)` method-level override.
7. Hook-local `schema_mode` and `project_output` options for hook output shape.

Method-level overrides apply only to the decorated subtransform output. Hook-local options apply only after that hook.

## Schema-Only Checks

`schema_only` validation checks:

- required columns;
- unexpected columns in strict mode;
- column order when strict projection requires it;
- Spark data types;
- nullable flags where Spark exposes them reliably;
- nested struct shape;
- array element type where available;
- map key and value types where available.

For aliased fields, validation checks the Spark column name, which is `alias` when supplied and the Python field name
otherwise. Aliases are schema-local, so each validation boundary uses the schema declared for that boundary.

It must not:

- call `count`;
- call `collect`;
- call `toPandas`;
- materialize data samples;
- add filtering actions only for validation;
- trigger row-level aggregations.

## Strictness and Projection

Validation has two related concerns:

- schema checking: decide whether the DataFrame shape is acceptable;
- projection: return a DataFrame with target fields in target order.

Strict schema validation rejects extra columns. `SchemaMode.ALLOW_EXTRA_COLUMNS` accepts extra columns for a hook output
when the hook declares that mode. If `project_output=True`, Structure projects the DataFrame back to the target schema
after the hook.

Rules:

- Compiled projections always emit target schema field order.
- Hook outputs use strict schema mode by default.
- `ALLOW_EXTRA_COLUMNS` permits extra columns only at the hook boundary that declared it.
- `project_output=True` removes extra columns and restores target field order.
- If `project_output=False`, extra columns may remain only when the active schema mode allows them.
- Final output validation is strict by default.

## Placement

Execution order for one subtransform:

1. Run `@before` hooks.
2. Execute compiled filters, joins, expressions, and projection for the subtransform.
3. Run `@after` hooks.
4. Validate the subtransform output when intermediate validation is enabled.
5. Apply any hook-specific projection required by the hook recipe at the exact hook boundary defined by the shared
   execution semantic contract.

Input validation happens before the first subtransform. Final output validation happens before returning the result.

Online and generated execution must use identical validation placement.

## Streaming Compatibility

`schema_only` validation may run for streaming DataFrames because it inspects schema metadata. Constraint validation is
streaming-compatible only when the specific constraint can run in Spark Structured Streaming without unsupported
operations.

Rules:

- Default validation must be compatible with v1 streaming-compatible transforms.
- Constraint modes must be rejected or warned for streaming when the constraint cost class is unsupported.
- Validation must not own streaming query lifecycle.

## Diagnostics

Validation diagnostics must include:

- phase;
- transform;
- subtransform when relevant;
- schema name;
- validation mode;
- field or column;
- expected shape;
- actual shape;
- problem;
- suggested fix;
- documentation link.

Example:

```text
RuntimeError VAL-E0702: DataFrame schema does not match Structure schema

Phase:
  input

Schema:
  OrderRaw

Column:
  total

Problem:
  The DataFrame is missing required column total.

Use:
  Fix the upstream read schema, rename the column before calling the transform, or update OrderRaw if the source
  contract changed.

See docs/specifications/ValidationSemantics.md
```

## Implementation Checklist

1. Parse validation configuration and defaults.
2. Normalize `validate_intermediate` and `intermediate_validation_mode`.
3. Record resolved validation policy on `TransformPlan` and `StepPlan`.
4. Generate or materialize target Spark schemas from `SchemaDef`.
5. Implement schema-only validation helpers.
6. Implement strict and allow-extra schema modes.
7. Implement projection to target schema order.
8. Place validation recipes through `ExecutionSemanticContract.md`.
9. Ensure online and generated execution consume the same validation recipes.
10. Add diagnostics with links to this specification.
11. Add tests proving schema-only validation does not scan rows.
12. Add tests for input, intermediate, output, hook allow-extra, and projection behavior.

## Acceptance Criteria

- Defaults resolve to schema-only input, intermediate, and output validation.
- `validate_intermediate = false` disables intermediate validation.
- Invalid validation mode values fail with allowed values.
- Input validation rejects missing required columns.
- Strict validation rejects unexpected columns.
- `SchemaMode.ALLOW_EXTRA_COLUMNS` accepts extra hook columns only at the declared hook boundary.
- `project_output=True` removes extra hook columns and restores target field order.
- Online and generated execution validate at the same points.
- Schema-only validation does not call Spark actions.
- Streaming-compatible transforms can use default schema-only validation.
- Validation diagnostics include phase, schema, field or column, problem, fix, and docs link.
