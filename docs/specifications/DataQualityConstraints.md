# Data Quality Constraints

## Purpose

Structure schemas already describe DataFrame shape: column names, field order, Spark types, nullability, and nested
structure. Data quality constraints describe facts about the data values inside that shape. Examples include accepted
values, ranges, regex-like patterns, decimal domains, uniqueness, referential checks, freshness, and row-count
expectations.

This specification defines the boundary. v1 validation is schema-first and schema-only by default. Richer data quality
checks are deferred and must be explicit because many of them trigger Spark actions, scans, aggregations, or joins.

## Validation Layers

Structure validation has three layers.

Compile-time checks run during `structure check` and `structure compile`. They inspect Structure source, schema
definitions, symbolic expressions, and compiler IR. They do not import PySpark, start Spark, scan data, or inspect live
DataFrames.

Schema-only runtime validation compares a live DataFrame schema against a generated Spark `StructType`. It checks
column names, missing columns, unexpected columns in strict mode, Spark data types, nullability where Spark exposes it
reliably, and nested shape. It must not scan rows.

Data-quality runtime validation evaluates value-level or dataset-level facts. It may add filters, aggregations, joins,
limits, counts, or other Spark work. Any check that can trigger Spark work must be explicit in source or configuration.

## v1 Boundary

v1 validation is schema-first.

Default intermediate validation uses:

```toml
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
```

`schema_only` must not trigger row scans. It validates shape only.

`schema_and_constraints` is reserved for explicit opt-in constraint validation:

```toml
input_validation_mode = "schema_and_constraints"
intermediate_validation_mode = "schema_and_constraints"
output_validation_mode = "schema_and_constraints"
```

Until Structure implements specific constraint families, this mode may be accepted as a configuration value while
diagnostics explain that row-level constraint checks are not yet available. Once implemented, checks that require Spark
actions must be visible in diagnostics, generated code, traceability, and documentation links.

Constraint declarations should bind to one or more validation phases: input, intermediate, output, or a narrower named
boundary. Phase validation modes are a project-level cost guard. A constraint is eligible to run only when it is bound
to the current phase and that phase's validation mode is `schema_and_constraints`.

## Constraint Families

Future constraint support should cover these families without making all of them v1 commitments.

Field-local constraints:

- enum or accepted values;
- numeric and date/time ranges;
- regex-like string patterns;
- string length limits;
- decimal domain rules such as non-negative money or bounded scale-sensitive amounts.

Schema-level constraints:

- unique keys;
- composite unique keys;
- conditional checks across fields;
- row-count minimum, maximum, or expected range;
- freshness checks for timestamp or date fields.

Cross-dataset constraints:

- referential checks against lookup inputs;
- foreign-key-like existence checks;
- anti-existence checks for excluded values.

Each constraint must declare whether it is compile-time only, schema-only at runtime, row-local, aggregation-based, or
join-based. This cost class determines whether the check can run in streaming mode and whether it can be enabled by
default.

## Source Model

The exact public DSL is deferred. Candidate forms may include field arguments, schema-level declarations, or decorators.
The implementation must not commit to a public constraint syntax until it can support diagnostics, generated code,
online/generated parity, and testing.

Conceptually, the compiler model should be able to represent:

```text
ConstraintDef
  name
  target
  phases
  kind
  expression
  severity
  cost
  source_path
  source_line
```

`target` may be a field, schema, transform input, subtransform output, final output, or referenced input. `phases`
declares where the constraint may run. `cost` classifies whether validation is cheap schema metadata work or Spark data
work.

## Generated Schema Reuse

Generated schema constants are supported caller-facing artifacts. A generated constant such as
`ORDER_ENRICHED_SCHEMA` is an ordinary PySpark `StructType` and may be imported by caller code.

Online execution must expose the same Spark `StructType` schemas without requiring generated files to exist. The
transform result makes the materialized schemas available by declared output name:

```python
transform = EnrichOrders(orders=orders_df, customers=customers_df, products=products_df)
result = transform.run(session)

output_schema = result.schema.enriched
same_schema = result.schema["enriched"]
result.enriched.write.mode("overwrite").parquet(target_path)
```

The online schema objects are produced from the same checked schema model as generated `*_SCHEMA` constants. They are
runtime objects, not generated files.

Caller-owned read example:

```python
from structure_generated.orders.pyspark.schemas.order import ORDER_RAW_SCHEMA

orders = spark.read.schema(ORDER_RAW_SCHEMA).parquet(source_path)
```

Caller-owned write preparation example:

```python
from structure_generated.orders.pyspark.schemas.order import ORDER_ENRICHED_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema, project_schema

assert_schema(df, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
df = project_schema(df, ORDER_ENRICHED_SCHEMA)
df.write.mode("overwrite").parquet(target_path)
```

Structure validates and projects DataFrames. The caller owns storage orchestration: `write`, `writeStream`, table
creation, partitioning, checkpoints, output modes, and storage-specific options.

Generated `*_SCHEMA` constants remain shape-only. Future data quality constraints must generate separate metadata or
runtime validation artifacts unless Spark-compatible metadata is deliberately added later. Adding constraint validation
must not silently change the meaning of existing `*_SCHEMA` constants.

## Diagnostics

Constraint diagnostics must name the cost and the reason for the check.

Required fields:

- diagnostic code;
- schema, field, transform, or validation point;
- constraint name or kind;
- cost class;
- problem;
- why it matters;
- suggested fix;
- documentation link.

Example:

```text
RuntimeError VAL-E0701: Data quality constraint failed

Schema:
  OrderEnriched

Constraint:
  non_negative_total

Cost:
  row-local Spark filter

Problem:
  The field total contains negative values.

Use:
  Repair the source data, filter invalid rows explicitly, or relax the constraint if negative totals are valid.

See docs/specifications/DataQualityConstraints.md
```

## Streaming Compatibility

Schema-only validation can be streaming-compatible because it inspects schema metadata only. Constraint validation is
streaming-compatible only when Spark can evaluate it without unsupported streaming operations. Aggregation-based,
join-based, row-count, and freshness checks should be classified as batch-only unless a narrower streaming rule proves
otherwise.

## Implementation Checklist

1. Keep v1 `schema_only` validation row-scan-free.
2. Add a constraint IR model only after the public behavior is specified.
3. Classify every constraint by cost and streaming compatibility.
4. Add validation recipes for supported constraints through the shared online/generated execution contract.
5. Keep generated `*_SCHEMA` constants shape-only.
6. Generate separate constraint metadata or runtime helpers when constraints are implemented.
7. Expose online materialized Spark schemas after transform execution.
8. Add diagnostics that link to this specification and explain Spark-action cost.
9. Add tests proving `schema_only` does not trigger data scans.
10. Add tests proving action-triggering checks are opt-in.
11. Add generated schema reuse tests for `spark.read.schema(...)`, online execution, and pre-write
    validation/projection.

## Acceptance Criteria

- v1 docs state that default validation is schema-first and schema-only at intermediate boundaries.
- `schema_and_constraints` is documented as opt-in and potentially more expensive.
- Generated schema constants are documented as caller-facing `StructType` artifacts.
- Online execution exposes equivalent materialized Spark schemas after `run(session)`.
- Generated schema constants remain shape-only even after future constraint metadata exists.
- Any future action-triggering data check is explicit in source or configuration.
- Diagnostics explain whether a validation failure came from schema shape or data-quality constraints.
