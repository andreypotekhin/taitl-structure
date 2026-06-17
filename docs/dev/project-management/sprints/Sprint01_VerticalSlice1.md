# Sprint 01: Vertical Slice 1

## Sprint Goal

Compile one simple schema-to-schema transform into a generated PySpark class and execute it against a small Spark DataFrame.

## Product Outcome

A developer can write one input schema, one output schema, and one transform method, then generate and run PySpark code.

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

- `Schema` declaration sufficient for primitive fields.
- `input(Schema)` declaration.
- `@transform` discovery.
- One public schema-returning method.
- Symbolic field references.
- Simple expression function: `to_decimal`.
- Projection IR.
- Generated class.
- Generated convenience function optional.
- Generated code syntax/import tests.
- One PySpark execution test.

### Out of Scope

- Runtime schema validation.
- Intermediate validation.
- Filtering.
- Hooks.
- Joins.
- Lineage.

## Relevant Specification Items

- As a developer, I can define schema classes.
- As a developer, I can declare a transform class with `@transform`.
- As a developer, I can declare named inputs using `input(Schema)`.
- As a developer, I can define a public schema-returning method as a subtransform.
- As a developer, I can generate one PySpark class per source transform class.
- As a developer, generated code uses Spark Column expressions rather than UDFs.

## Deliverables

- Minimal schema DSL.
- Minimal transform discovery.
- Minimal symbolic execution.
- Minimal IR.
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
9. Implement generated class emitter.
10. Add generated-code snapshot test.
11. Add PySpark execution test.

## Acceptance Criteria

- Example transform compiles.
- Generated code imports successfully.
- Generated class runs against small Spark DataFrame.
- Output values match expected results.
- Generated code contains no UDF, RDD, collect, or row-wise map.
- `structure check` reports success for the fixture.

## Demo Script

```bash
structure compile --src tests/fixtures/vertical_slice_1/structure/src --out /tmp/structure/generated
pytest tests/test_vertical_slice_1.py
```

## Compile-Time Performance Metric

Track cold compile time for the vertical slice fixture.

Target:

- Single-transform compile: under 1 second excluding Spark startup.

## Risks

- Symbolic execution may be over-engineered too early.
- Generated import paths may need adjustment once package layout is finalized.
