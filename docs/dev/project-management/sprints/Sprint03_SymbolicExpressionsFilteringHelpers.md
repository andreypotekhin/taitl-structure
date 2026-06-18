# Sprint 03: Symbolic Expressions, Filtering, and Helpers

## Sprint Goal

Make the source DSL practically useful by adding common expressions, `where(...)` filtering, `@expr_fn` helpers, and structured unsupported-code errors.

## Product Outcome

Developers can write useful compiled transforms without falling back to PySpark hooks, and unsupported Python produces actionable diagnostics.

## Scope

### In Scope

- Expression functions: `lower`, `trim`, `upper`, `coalesce`, `when`, `to_decimal`, casts.
- Binary comparisons.
- Boolean combinations.
- `where(...)` filtering.
- Class-local `@expr_fn` helpers without `self` parameter.
- Module-level `@expr_fn` helpers.
- Schema base overlay construction with `SchemaClass.base(row)(...)`.
- Structured unsupported-code errors.
- Error suggestions: direct DSL, `@expr_fn`, hook, config workaround when applicable.
- Performance guardrail tests.

### Out of Scope

- Joins.
- Hooks execution.
- Aggregations/windowing.
- Spark HOFs; defer to v2.

## Relevant Specification Items

- As a developer, I can compile field references to Spark Columns.
- As a developer, I can use `where(...)` for filtering.
- As a developer, I can define module-level `@expr_fn` helpers.
- As a developer, I can define class-local `@expr_fn` helpers without a `self` parameter.
- As a developer, I can construct an output schema from inherited base schema rows plus explicit overrides.
- As a developer, I receive structured compiler errors for unsupported Python.
- As a developer, I receive alternatives including DSL functions, `@expr_fn`, hooks, and config workarounds.
- As a developer, compiled paths do not silently fall back to UDFs.

## Example Source

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))


def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(order.customer_id.is_not_null())

    return OrderNormalized(
        id=order.id,
        customer_id=self.clean_id(order.customer_id),
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

## Example Generated PySpark

```python
df = orders.where(
    F.col("id").isNotNull()
    & F.col("customer_id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
    F.col("total").cast("decimal(12,2)").alias("total"),
)
```

## Engineering Tasks

1. Implement expression function registry.
2. Implement binary comparison expressions.
3. Implement boolean expression composition.
4. Implement `where(...)` context capture.
5. Implement `@expr_fn` decorator.
6. Support class-local helper invocation through `self`.
7. Implement schema base overlay construction and lower it to projection IR.
8. Implement unsupported operation traps.
9. Implement structured compiler error model.
10. Add detailed error message rendering.
11. Add static generated-code performance scans.

## Acceptance Criteria

- Filtering compiles to DataFrame `.where(...)`.
- Expression helpers inline into generated PySpark.
- Unsupported `.strip().lower()` fails with a detailed error.
- Error suggests `lower(trim(...))`.
- Error suggests creating `@expr_fn`.
- Error suggests a hook escape hatch.
- `SchemaClass.base(row)(overrides...)` compiles to the same explicit projection as the equivalent full constructor.
- Multiple-base overlays map source rows to direct schema bases in declaration order.
- Generated code contains no UDFs.

## Demo Script

```bash
structure check --source-root tests/fixtures/expressions/src
structure compile --source-root tests/fixtures/expressions/src --out /tmp/generated
pytest tests/test_expressions_filtering.py
```

## Compile-Time Performance Metric

Track symbolic execution time for expression-heavy fixtures.

Target:

- 100 simple expressions compile in under 1 second excluding module import overhead.

## Risks

- Operator overloading may allow Python boolean mistakes.
- Error source snippets may require LibCST or inspect integration.
