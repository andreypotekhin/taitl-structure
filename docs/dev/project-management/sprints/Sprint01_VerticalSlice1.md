# Sprint 01: v1 First Executable Slice

## Sprint Goal

Run one schema-to-schema transform online through `StructureSession`, emit the equivalent generated PySpark class, and
prove both paths produce the same rows for the same small input.

## Product Outcome

A developer can write one input schema, one output schema, and one transform method with filtering and one expression
helper, then run it with:

```python
NormalizeOrders(orders=orders_df).run(session)
```

Generated PySpark can be emitted and compared against online execution as the same executable contract.

## Example Source

```python
@transform
class NormalizeOrders(Transform):

    orders = input(OrderRaw)
    normalized = output(OrderNormalized)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())

        return OrderNormalized(
            id=self.clean_id(order.id),
            customer_id=self.clean_id(order.customer_id),
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )
```

## Example Online Execution

```python
session = StructureSession(spark=spark)

result = NormalizeOrders(
    orders=orders_df,
).run(session)
```

## Example Generated PySpark

```python
class NormalizeOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx

    def run(self, *, orders):
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")

        total = F.coalesce(F.col("total").cast("decimal(12,2)"), F.lit(0).cast("decimal(12,2)"))
        df = orders.where(
            F.col("id").isNotNull()
            & F.col("customer_id").isNotNull()
        ).select(
            F.lower(F.trim(F.col("id"))).alias("id"),
            F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
            total.alias("total"),
        )
        return df
```

## Scope

### In Scope

- Sprint 00 spike outcomes incorporated into the implementation approach.
- `Structure` declaration sufficient for primitive fields using explicit type objects such as `String()`.
- `input(Structure)` declaration.
- `@transform` discovery.
- One public schema-returning method.
- Symbolic field references.
- Simple expression functions: `lower`, `trim`, `coalesce`, and `to_decimal`.
- One class-local `@expr_fn` helper.
- `where(...)` filtering.
- Projection IR.
- Shared PySpark execution recipes for online/generated parity.
- `StructureSession`.
- Deferred transform invocation with named inputs.
- Online PySpark runner for projection IR.
- Generated class.
- Generated convenience function optional.
- Input validation.
- Online/generated parity test.
- Generated code syntax/import tests.
- Online execution test.
- One PySpark execution test.

### Out of Scope

- Intermediate validation.
- Output validation.
- Hooks.
- Joins.
- Traceability.
- Schema base overlay construction.

## Relevant Specification Items

- As a developer, I can define schema classes.
- As a developer, I can declare a transform class with `@transform`.
- As a developer, I can declare named inputs using `input(Structure)`.
- As a developer, I can define a public schema-returning method as a subtransform.
- As a developer, I can construct a transform invocation with named input DataFrames.
- As a developer, I can run the transform online through `StructureSession`.
- As a developer, I can filter rows with `where(...)`.
- As a developer, I can define one class-local `@expr_fn` helper.
- As a developer, input schemas are validated before execution.
- As a developer, I can generate one PySpark class per source transform class.
- As a developer, generated code uses Spark Column expressions rather than UDFs.
- As a developer, online and generated execution produce the same result for the first v1 fixture.

## Deliverables

- Minimal schema DSL.
- Minimal transform discovery.
- Minimal symbolic execution.
- Minimal IR.
- Minimal expression helper support.
- Minimal filtering support.
- Minimal shared PySpark execution recipe layer.
- Minimal `StructureSession`.
- Minimal online PySpark runner.
- Minimal PySpark code emitter.
- Generated class template.
- Input validation helper.
- Spark execution test fixture.

## Engineering Tasks

1. Implement primitive schema fields.
2. Implement `@transform` metadata.
3. Implement `input(...)` metadata.
4. Implement symbolic row proxy.
5. Implement field reference expression.
6. Implement schema construction capture.
7. Implement `to_decimal` expression.
8. Implement `lower`, `trim`, and `coalesce` expressions.
9. Implement class-local `@expr_fn` helper support for one helper.
10. Implement `where(...)` filtering.
11. Implement projection IR.
12. Implement shared PySpark execution recipes for projection and filtering IR.
13. Implement input validation.
14. Implement `StructureSession`.
15. Implement deferred transform invocation input binding.
16. Implement online PySpark runner for the shared recipes.
17. Implement generated class emitter from the shared recipes.
18. Add online execution test.
19. Add generated-code snapshot test.
20. Add online/generated parity test.
21. Add PySpark execution test.

## Acceptance Criteria

- Example transform compiles.
- Example transform runs online through `StructureSession`.
- Input schema mismatch fails before transform execution.
- Generated code imports successfully.
- Generated class runs against small Spark DataFrame.
- Online and generated execution return the same rows.
- Output values match expected results.
- Online and generated execution consume the same PySpark recipe layer.
- Generated code contains no UDF, RDD, collect, or row-wise map.
- `structure check` reports success for the fixture.

## Progress

- [x] (2026-06-21) Minimal schema DSL, transform discovery, symbolic execution, projection/filter recipes, generated
  transform rendering, and generated-file checks are implemented.
- [x] (2026-06-21) Public `StructureSession` is implemented with deferred named-input invocation, missing-input
  diagnostics, online runner delegation, and generated runner delegation.
- [x] (2026-06-21) Generated mode imports and calls generated `*Generated` classes through the same transform invocation
  API in Spark-free tests.
- [x] (2026-06-23) Live online PySpark recipe interpretation is implemented through the shared target recipe layer.
- [x] (2026-06-23) Local Spark online/generated row parity is covered by the opt-in PySpark integration matrix.

## Demo Script

```bash
structure check --source-root tests/fixtures/vertical_slice_1/src
structure compile --source-root tests/fixtures/vertical_slice_1/src --out /tmp/generated
pytest tests/test_vertical_slice_1.py
```

## Compile-Time Performance Metric

Track cold compile time for the vertical slice fixture.

Target:

- Single-transform compile: under 1 second excluding Spark startup.

## Risks

- Symbolic execution may be over-engineered too early.
- Generated import paths may still need adjustment if Sprint 00 import-path proof exposes edge cases.
