# Design: Compileability Checker

## Purpose

The checker ensures compiled subtransforms can generate efficient PySpark expressions and that schema flow is valid.

## Checks

- field existence
- type compatibility
- nullability compatibility
- source-order schema flow
- `where(...)` predicates are boolean
- join conditions are boolean
- `join_one(...)` uniqueness warnings
- hook signature validation
- streaming compatibility
- performance guardrails

## Error Style

Errors should include:

- code
- transform class
- subtransform
- field
- source expression
- problem
- rationale
- suggested DSL fix
- `@expr_fn` helper fix
- hook workaround
- config workaround when one exists

## Example Error

```text
CompileError STRUCT-E1004: Unsupported expression

Transform:
  EnrichOrders

Subtransform:
  normalize

Output field:
  OrderNormalized.customer_id

Source expression:
  order.customer_id.strip().lower()

Problem:
  Python string methods cannot be compiled to Spark Column expressions.

Why this matters:
  Silent fallback to UDFs would reduce Spark optimizer visibility.

Use:
  customer_id=lower(trim(order.customer_id))

For reuse:
  @expr_fn
  def clean_id(value):
      return lower(trim(value))

Hook workaround:
  @after(normalize)
  def clean_customer_id(self, *, df, spark, ctx):
      return df.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))

Configuration workaround:
  None. Unsupported Python methods are not allowed in compiled transforms.
```

## Config Workarounds

Only suggest config when it really applies. Examples:

- `validate_intermediate = false` for intermediate validation failures.
- `lineage = "none"` for compiler provenance and static dataflow lineage performance.
- `strict_performance = false` for hook lint warnings, not compiled subtransform fallback.

## Compile-Time Performance

Checks should run against IR and avoid PySpark imports, Java, SparkSession creation, Spark cluster access, and Spark
startup. Use static target capability metadata for PySpark version-specific rules, and provide a profiling hook to
identify slow checks.
