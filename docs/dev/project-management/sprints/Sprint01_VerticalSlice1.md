# Sprint 01: Vertical Slice 1

## Sprint Goal

Run one simple schema-to-schema transform online through `StructureSession`, and optionally emit the equivalent
generated PySpark class.

## Product Outcome

A developer can write one input schema, one output schema, and one transform method, then run it with:

```python
NormalizeOrders(orders=orders_df).run(session)
```

Generated PySpark can still be emitted and compared as an optional artifact.

## Example Source

```python
@transform
class NormalizeOrders(Transform):

    orders = input(OrderRaw)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(
            id=order.id,
            customer_id=order.customer_id,
            total=to_decimal(order.total, precision=12, scale=2),
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
        df = orders.select(
            F.col("id").alias("id"),
            F.col("customer_id").alias("customer_id"),
            F.col("total").cast("decimal(12,2)").alias("total"),
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
- Simple expression function: `to_decimal`.
- Projection IR.
- `StructureSession`.
- Deferred transform invocation with named inputs.
- Online PySpark runner for projection IR.
- Generated class.
- Generated convenience function optional.
- Generated code syntax/import tests.
- Online execution test.
- One PySpark execution test.

### Out of Scope

- Runtime schema validation.
- Intermediate validation.
- Filtering.
- Hooks.
- Joins.
- Lineage.
- Schema base overlay construction.

## Relevant Specification Items

- As a developer, I can define schema classes.
- As a developer, I can declare a transform class with `@transform`.
- As a developer, I can declare named inputs using `input(Structure)`.
- As a developer, I can define a public schema-returning method as a subtransform.
- As a developer, I can construct a transform invocation with named input DataFrames.
- As a developer, I can run the transform online through `StructureSession`.
- As a developer, I can generate one PySpark class per source transform class.
- As a developer, generated code uses Spark Column expressions rather than UDFs.

## Deliverables

- Minimal schema DSL.
- Minimal transform discovery.
- Minimal symbolic execution.
- Minimal IR.
- Minimal `StructureSession`.
- Minimal online PySpark runner.
- Minimal PySpark code emitter.
- Generated class template.
- Spark execution test fixture.

## Engineering Tasks

1. Implement primitive schema fields.
2. Implement `@transform` metadata.
3. Implement `input(...)` metadata.
4. Implement symbolic row proxy.
5. Implement field reference expression.
6. Implement schema construction capture.
7. Implement `to_decimal` expression.
8. Implement projection IR.
9. Implement `StructureSession`.
10. Implement deferred transform invocation input binding.
11. Implement online PySpark runner for projection IR.
12. Implement generated class emitter.
13. Add online execution test.
14. Add generated-code snapshot test.
15. Add PySpark execution test.

## Acceptance Criteria

- Example transform compiles.
- Example transform runs online through `StructureSession`.
- Generated code imports successfully.
- Generated class runs against small Spark DataFrame.
- Output values match expected results.
- Generated code contains no UDF, RDD, collect, or row-wise map.
- `structure check` reports success for the fixture.

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
