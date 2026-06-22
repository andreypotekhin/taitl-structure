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
- hook opacity note when a workaround moves logic out of the compiler-visible path
- config workaround when one exists

## Example Error

```text
CompileError DSL-E0401: Unsupported expression

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

Hook note:
  Hooks are supported for arbitrary PySpark, but they are opaque to compileability checks and traceability. Prefer the DSL or
  @expr_fn form when the expression can stay compiler-visible.

Configuration workaround:
  None. Unsupported Python methods are not allowed in compiled transforms.
```

## Config Workarounds

Only suggest config when it really applies. Examples:

- `validate_intermediate = false` for intermediate validation failures.
- `traceability = "none"` for compiler provenance and static dataflow traceability performance.
- `strict_performance = false` for hook lint warnings, not compiled subtransform fallback.

## Extension Boundaries

The checker should treat `@expr_fn` helpers as the preferred project extension point for reusable expression logic.
They stay inside symbolic execution and therefore remain visible to generated code, traceability, backend capability checks,
and diagnostics.

Hooks are supported extension points for arbitrary PySpark DataFrame code, but the checker must treat hook bodies as
opaque. It validates hook attachment, signature, lifecycle options, and configured safety declarations; it does not
infer field traceability or compileability from code inside the hook.

Compiler registries for backend capabilities, diagnostics, schema type adapters, validation policies, and hook lint
rules are internal until a public contract is deliberately introduced. Diagnostics should not tell users to monkey-patch
or register custom implementations in those internals.

## Compile-Time Performance

Checks should run against IR and avoid PySpark imports, Java, SparkSession creation, Spark cluster access, and Spark
startup. Use static target capability metadata for PySpark version-specific rules, and provide a profiling hook to
identify slow checks.
