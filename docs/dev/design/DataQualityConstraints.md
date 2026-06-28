# Design: Data Quality Constraints

## Purpose

C26 is resolved by separating schema shape from data quality. Structure v1 should be honest: it enforces declared
DataFrame shape by default, while value-level and dataset-level quality checks are a future opt-in model because they
can make Spark scan, join, aggregate, or count data.

## Design Boundary

The base schema model owns field order, field names, types, nullability, inheritance, and generated Spark `StructType`
constants. Data quality constraints sit beside that shape model. They should use `SchemaDef.constraints` or an
equivalent side collection, but they should not alter generated `*_SCHEMA` constants.

Generated schema constants remain ordinary PySpark `StructType` values. Caller code may import them for reads, runtime
validation, projection before writes, and tests. They do not carry executable data-quality policy unless a future design
intentionally adds Spark-compatible metadata without changing shape semantics.

## Constraint Model

The future internal model should support:

```text
ConstraintDef
  name
  owner
  target
  phases
  kind
  expression
  severity
  cost
  streaming_compatibility
  source
```

`owner` identifies the schema or transform boundary. `target` identifies a field, schema, input, subtransform output,
or final output. `phases` records whether the constraint may run at input, intermediate, output, or a narrower named
boundary. `kind` distinguishes enum, range, regex, decimal domain, unique key, referential check, freshness, and
row-count policy.

`cost` is required. Suggested cost classes:

- `compile_time`, for facts proven without runtime data;
- `schema_only`, for metadata checks against `DataFrame.schema`;
- `row_local`, for filters or projections that inspect rows without aggregation;
- `aggregation`, for counts, uniqueness, and row-count policies;
- `join`, for referential checks against another DataFrame.

## Validation Recipes

Once constraints are implemented, they should lower through the shared PySpark semantic contract. Online execution and
generated code must consume the same validation recipes so constraint behavior cannot drift between runtime modes.

Conceptual recipe:

```text
PySparkConstraintValidationRecipe
  validation_point
  schema
  constraint
  cost
  failure_mode
  diagnostic
```

Schema-only validation remains the default recipe for v1 intermediate boundaries. Constraint recipes are emitted only
when the constraint is bound to the current validation phase and that phase is configured with
`schema_and_constraints`.

## Diagnostics

Diagnostics must make cost visible. A user should be able to tell whether validation failed because the DataFrame shape
was wrong or because Structure ran an explicit data-quality check.

Every data-quality diagnostic should include:

- validation point;
- constraint name and kind;
- cost class;
- whether the check can trigger a Spark action;
- suggested source or configuration change;
- link to [DataQualityConstraints.md](../../specifications/DataQualityConstraints.md).

## Generated Schema Reuse

Generated schema modules are part of the generated-code public surface. Online execution must also expose equivalent
materialized Spark schemas without requiring generated files. Both surfaces are intentionally useful outside transform
internals.

```python
from structure_generated.orders.pyspark.schemas.order import ORDER_ENRICHED_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema, project_schema

assert_schema(df, ORDER_ENRICHED_SCHEMA, name="OrderEnriched", mode="strict")
df = project_schema(df, ORDER_ENRICHED_SCHEMA)
df.write.mode("overwrite").parquet(target_path)
```

Online execution should expose the same shape through the transform result:

```python
transform = EnrichOrders(orders=orders_df, customers=customers_df, products=products_df)
result = transform.run(session)

schema = result.schema.enriched
result.enriched.write.mode("overwrite").parquet(target_path)
```

Structure should not add a v1 write orchestration helper. Storage writes belong to the caller because partitioning,
mode, table format, checkpoints, and environment policy are deployment concerns.

## Implementation Notes

- Keep `schema_only` validation row-scan-free.
- Keep generated `*_SCHEMA` constants deterministic and shape-only.
- Materialize equivalent Spark schemas during online execution and expose them from the transform invocation.
- Add separate generated constraint metadata when constraints are implemented.
- Classify streaming compatibility from constraint cost.
- Add tests proving generated schemas can be imported and used by caller code.
- Add tests proving action-triggering validation is opt-in.
