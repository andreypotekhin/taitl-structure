# UDF API

Use a user-defined function (UDF) only when the typed expression API cannot express the calculation. UDFs are explicit
Python execution boundaries: Structure checks their declaration and call shape, while Spark runs the callback. Prefer
ordinary Structure helpers for logic that can be represented with native Spark expressions.

## Declarations

| Structure API | Purpose | Required declaration |
| --- | --- | --- |
| `@special(type="udf", return_type=..., nullable=...)` | Scalar Python function called for rows | Concrete PySpark-plugin result type and explicit nullability |
| `@special(type="arrow_udf", return_type=..., function_type="scalar")` | Arrow batches for scalar results | Concrete type; `scalar` is the default mode |
| `@special(type="arrow_udf", return_type=..., function_type="scalar_iter")` | Iterator over Arrow input batches | Concrete type and one iterator callback parameter |
| `@special(type="arrow_udf", return_type=..., function_type="grouped_agg")` | Group or window aggregate callback | Concrete type and scalar result per group/window |

Scalar Python UDFs accept `nullable` as an author contract. Arrow declarations also accept `nullable=True` and optional
`deterministic: bool`; leaving `deterministic` out preserves Spark's default. Arrow inputs are typed from the call-site
expressions. PyArrow is needed by the driver and workers at execution, but need not be imported when compiling.

## Boundaries

- Prefer a symbolic helper or native Structure operation when one expresses the same calculation.
- Scalar Python UDFs are row-local. Their Python body is opaque and Structure cannot infer its return type or behavior.
- Arrow UDFs require the exact PySpark `>=4.1,<4.2` profile. Ordinary PySpark and Spark Connect have batch support;
  ordinary streaming supports scalar and scalar-iterator forms. See the [compatibility reference](../reference/UDF.ref.md#compatibility).
- Pandas UDFs, UDTFs, Arrow UDTFs, arbitrary callback registration, and implicit Python-to-UDF conversion are not
  Structure UDF contracts. Use a caller-owned native PySpark boundary when those APIs are needed.
- Every UDF is opaque to Spark SQL optimization. The `DSL-W0403` warning is enabled by default for scalar Python UDFs;
  the configuration changes the warning, not the execution boundary.

See the [UDF reference](../reference/UDF.ref.md) for complete examples and [UDF background](../background/UDF.back.md)
for execution and optimization behavior.
