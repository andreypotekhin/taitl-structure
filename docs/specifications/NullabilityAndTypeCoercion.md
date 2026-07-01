# Nullability and Type Coercion

## Purpose

Structure schemas declare field types and nullability. Transform methods produce expressions assigned to those fields.
The compiler must know when an assignment is safe, when a filter has made a value non-null, when a Python literal can be
used as an expression, and when a developer must write an explicit conversion helper.

The goal is to feel natural to PySpark users without hiding important data-quality choices. Numeric widening and typed
literals should not make common code noisy. Parsing conversions such as string-to-decimal should remain visible.

## Spark SQL Configuration

Structure records Spark SQL assumptions under `[tool.structure]` using Spark's own dotted key names:

```toml
[tool.structure]
spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

Defaults:

- `spark.sql.ansi.enabled = true`
- `spark.sql.storeAssignmentPolicy = "ANSI"`

Rules:

- `spark.sql.ansi.enabled` must be a boolean.
- `spark.sql.storeAssignmentPolicy` must be one of `ANSI`, `LEGACY`, or `STRICT`.
- v1 specifies detailed assignment rules for the default ANSI policy.
- `LEGACY` and `STRICT` may be parsed in v1, but diagnostics may say that detailed checking for those policies is
  deferred unless implemented.
- These settings are compiler assumptions and generated-runtime expectations.
- Structure does not start Spark sessions or mutate Spark session configuration in v1.

Generated runtime code may assert that an existing Spark session matches these assumptions if runtime assertion support
is enabled later. If such an assertion is added, the error should tell users to set the corresponding Spark session
configuration or update `[tool.structure]`.

## Painpoint Analysis

Exact-only assignment is simple to implement but unpleasant to use. It would reject ordinary cases such as assigning an
integer expression to a long field, assigning `0` inside `coalesce(decimal_value, 0)`, or widening a compatible decimal.
Those cases are common in PySpark work and do not hide business intent.

Fully implicit casting is also wrong for Structure. Assigning a string field to a decimal output silently asks Spark to
parse data and choose runtime failure or null behavior based on Spark mode. That is a business rule and should be
visible in source code.

The v1 policy is therefore balanced:

- Accept Spark-ANSI-compatible assignment coercions for unsurprising type widening and typed literals.
- Require explicit helper calls for semantic parsing conversions.
- Reject lossy, surprising, or policy-dependent conversions unless explicitly expressed.
- Explain the shortest fix in diagnostics.

## Nullability Model

Every expression has static nullability metadata:

- `nullable=True`: the expression may produce null.
- `nullable=False`: the expression is known by the compiler to be non-null.

Static nullability is conservative. It is based on schema declarations, literal values, expression helper rules, and
simple filter narrowing. It does not scan data and does not try to prove arbitrary Python conditions.

## Field and Literal Nullability

Field references inherit nullability from their declared schema field:

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False)
    total = field(String(), nullable=True)
```

`order.id` is non-null. `order.total` is nullable.

Python literals are valid Structure source expressions:

```python
coalesce(order.total, "0")
coalesce(to_decimal(order.total, precision=12, scale=2), 0)
```

Literal rules:

- `None` is a nullable untyped null literal.
- `str` is non-null `String()`.
- `bool` is non-null `Boolean()`.
- `int` is non-null `Integer()` unless the value is outside 32-bit range, in which case it is `Long()`.
- `float` is non-null `Double()` by default because Python `float` is double precision.
- `datetime.date` is non-null `Date()`.
- `datetime.datetime` is non-null `Timestamp()`.

Generated PySpark may lower literals to `F.lit(...)`. Users should not need to write `lit(...)` in Structure source.

## Null-Intolerant and Null-Aware Expressions

Most expression helpers are null-intolerant: if any required input is nullable, the result is nullable. Examples:

- `lower(value)`
- `upper(value)`
- `trim(value)`
- `to_decimal(value, precision=12, scale=2)`

Null-aware helpers have specific rules:

- `is_null(value)` returns non-null `Boolean()`.
- `is_not_null(value)` returns non-null `Boolean()`.
- `coalesce(a, b, ...)` returns the first non-null runtime value. It is statically non-null when at least one
  argument is statically non-null. It is nullable only when all arguments are nullable.
- `when(condition, value).otherwise(fallback)` is non-null only when all result branches are statically non-null.

Spark's SQL null semantics remain the inspiration for generated behavior. Normal comparisons involving null may produce
null, while null-safe equality is a separate explicit operation if supported.

Basic row-local arithmetic in v1 supports `+`, `-`, and `*`. Result typing is intentionally conservative and follows
the left operand's Structure type until fuller numeric result formulas are specified.

## Filter Narrowing

`where(expr.is_not_null())` narrows a simple field reference after the filter in the same subtransform:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())

    return OrderNormalized(id=order.id)
```

If `OrderNormalized.id` is non-nullable, this is accepted even when `OrderRaw.id` is nullable.

Narrowing rules:

- Narrow direct field references such as `order.id`.
- Narrow direct aliases if the compiler represents them explicitly.
- Do not infer broad facts from arbitrary boolean expressions in v1.
- Narrowing applies after the `where(...)` call in the same subtransform.
- Narrowing does not cross hook boundaries unless a later spec defines hook postconditions.

## Output Nullability Assignment

A nullable expression cannot feed a non-nullable output field unless the compiler can see that it has been made
non-null.

Rejected:

```python
return OrderNormalized(
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Accepted with a default:

```python
return OrderNormalized(
    total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
)
```

Accepted with a guard:

```python
where(order.total.is_not_null())

return OrderNormalized(
    total=to_decimal(order.total, precision=12, scale=2),
)
```

The guard example proves source nullability only. Runtime parsing failures follow the selected helper and Spark SQL
configuration.

## Type Model

Every expression has a Structure type. Assignment compatibility compares the expression type with the output field type.

The v1 scalar types are those from the schema syntax specification:

- `String()`
- `Integer()`
- `Long()`
- `Float()`
- `Double()`
- `Boolean()`
- `Date()`
- `Timestamp()`
- `Decimal(precision, scale)`

Arrays and structs follow the same assignment idea recursively when implemented in v1.

## Assignment Compatibility

An expression can be assigned to an output field when both nullability and type compatibility pass.

In the default Spark SQL ANSI policy, v1 should accept:

- exact type matches;
- untyped `None` for nullable fields;
- integer literals assignable to `Integer()`, `Long()`, or compatible `Decimal(...)` fields;
- floating literals assignable to `Double()` fields, or to `Float()` fields when target context is explicit;
- `Integer()` expressions assigned to `Long()` fields;
- `Float()` expressions assigned to `Double()` fields;
- integer expressions assigned to compatible `Decimal(...)` fields;
- decimal expressions assigned to a decimal field with enough integer digits and scale;
- values assigned to `String()` only when the source type is already `String()` or the conversion is explicitly
  requested.

v1 should reject:

- nullable expression to non-nullable field;
- `String()` to numeric, date, or timestamp fields without an explicit helper;
- numeric, date, timestamp, or boolean values to `String()` without an explicit helper;
- `Double()` expressions assigned to `Float()` fields without an explicit helper;
- decimal narrowing that can lose integer digits or scale;
- boolean-to-numeric and numeric-to-boolean assignment;
- array or struct assignment when element or field compatibility fails.

If `spark.sql.storeAssignmentPolicy = "STRICT"`, the checker should reject any assignment that can lose precision or
truncate data. If detailed strict checking is not implemented in v1, reject non-exact assignments with a clear message.

If `spark.sql.storeAssignmentPolicy = "LEGACY"`, the checker may allow Spark-valid casts only when the implementation
can explain the behavior. Until then, prefer a diagnostic that says legacy assignment checking is not yet implemented
and asks for an explicit helper.

## Decimal Rules

`Decimal(precision, scale)` means a number can have at most `precision` total digits, with `scale` digits after the
decimal point.

Assignment from `Decimal(p1, s1)` to `Decimal(p2, s2)` is accepted when:

- `s2 >= s1`; and
- `p2 - s2 >= p1 - s1`.

This preserves both fractional and integer digits.

`Integer()` to `Decimal(p, s)` is accepted when `p - s >= 10`, because a 32-bit integer may need ten integer digits
including sign range. `Long()` to `Decimal(p, s)` is accepted when `p - s >= 19`.

Decimal arithmetic result typing may follow Spark's formulas later. v1 only needs assignment compatibility and helper
result types used by current expression helpers.

## Explicit Conversion Helpers

Semantic parsing conversions must be explicit:

```python
to_decimal(order.total, precision=12, scale=2)
to_date(order.event_date, format="yyyy-MM-dd")
to_timestamp(order.event_time, format="yyyy-MM-dd HH:mm:ss")
```

Rules:

- `to_decimal(value, precision=P, scale=S)` returns `Decimal(P, S)`.
- Date and timestamp parsing helpers return `Date()` and `Timestamp()` respectively when implemented.
- Parsing helpers preserve input nullability unless their own semantics guarantee otherwise.
- Tolerant helpers may be added later, such as `try_to_timestamp(...)`, and should return nullable results.

Invalid parse behavior is governed by the helper and the configured Spark SQL assumptions. Structure should not pretend
that a parsing conversion is non-null just because the target field is non-null.

## Least Common Type for Helpers

Helpers that combine multiple values, especially `coalesce(...)`, need a result type.

For v1, use a small Structure type join rule:

- untyped `None` adopts the other argument type;
- exact matches keep that type;
- `Integer()` and `Long()` produce `Long()`;
- `Float()` and `Double()` produce `Double()`;
- integral values combined with `Float()` or `Double()` produce the floating type;
- integer values and compatible decimal values produce the compatible decimal type;
- decimal values use the smallest decimal that preserves integer digits and scale;
- incompatible types are rejected with a diagnostic.

When a helper appears in an output assignment, the target field may provide context for untyped literals:

```python
total=coalesce(to_decimal(order.total, precision=12, scale=2), 0)
```

The literal `0` may be typed as `Decimal(12, 2)` because the other argument and target field agree on that decimal type.

## Diagnostics

Diagnostics must include:

- error code;
- transform class when available;
- subtransform when available;
- output field;
- source expression;
- expected field type and nullability;
- actual expression type and nullability;
- concise fix;
- link to this specification.

Nullable-to-non-nullable example:

```text
CompileError SCHEMA-E0301: Nullable expression assigned to non-nullable field

Output field:
  OrderNormalized.total: Decimal(12, 2), nullable=False

Source expression:
  to_decimal(order.total, precision=12, scale=2)

Problem:
  The expression may produce null, but the output field is non-nullable.

Use:
  total=coalesce(to_decimal(order.total, precision=12, scale=2), 0)

See docs/specifications/NullabilityAndTypeCoercion.md
```

Parsing conversion example:

```text
CompileError SCHEMA-E0302: Explicit conversion required

Output field:
  OrderNormalized.total: Decimal(12, 2), nullable=True

Source expression:
  order.total: String(), nullable=True

Problem:
  String-to-decimal parsing is data-quality logic and must be visible in Structure source.

Use:
  total=to_decimal(order.total, precision=12, scale=2)

See docs/specifications/NullabilityAndTypeCoercion.md
```

Type mismatch example:

```text
CompileError SCHEMA-E0303: Incompatible output field type

Output field:
  OrderNormalized.is_paid: Boolean(), nullable=False

Source expression:
  order.payment_count: Integer(), nullable=False

Problem:
  Integer values do not assign to Boolean fields under the configured Spark SQL assignment policy.

Use:
  is_paid=order.payment_count > 0

See docs/specifications/NullabilityAndTypeCoercion.md
```

## Implementation Checklist

1. Parse `spark.sql.ansi.enabled` and `spark.sql.storeAssignmentPolicy` from `[tool.structure]`.
2. Add default values to the seed configuration.
3. Carry Structure type and nullability on expression IR nodes.
4. Infer metadata for field references and Python literals.
5. Infer metadata for current expression helpers.
6. Implement `coalesce(...)` least-common-type and nullability rules.
7. Implement filter narrowing for direct `is_not_null()` guards.
8. Implement output assignment compatibility checks.
9. Add diagnostics with links to this specification.
10. Lower source Python literals to generated PySpark `F.lit(...)` where needed.
11. Add tests for accepted and rejected assignments.
12. Update user docs to avoid source-level `lit(...)` in Structure examples.

## Acceptance Criteria

- `spark.sql.ansi.enabled = true` is accepted under `[tool.structure]`.
- `spark.sql.storeAssignmentPolicy = "ANSI"` is accepted under `[tool.structure]`.
- A nullable expression assigned to a non-nullable output field is rejected.
- The same expression is accepted when guarded by `where(expr.is_not_null())`.
- `coalesce(to_decimal(order.total, precision=12, scale=2), 0)` is accepted for a non-nullable decimal output.
- Users can write Python literals in Structure source; generated PySpark uses `F.lit(...)` where Spark requires it.
- `Integer()` assigned to `Long()` is accepted under the default ANSI assignment policy.
- `Float()` assigned to `Double()` is accepted under the default ANSI assignment policy.
- Python `float` literals infer `Double()` unless a `Float()` target supplies explicit context.
- Compatible decimal widening is accepted.
- Decimal narrowing is rejected.
- `String()` assigned directly to `Decimal(...)` is rejected with a `to_decimal(...)` suggestion.
- Unsupported assignment policies produce clear diagnostics rather than silent miscompilation.
