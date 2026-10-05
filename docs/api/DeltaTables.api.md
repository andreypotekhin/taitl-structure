# Delta Tables API

Structure can compile typed mutations against an existing Delta table. The caller creates the table, provisions its
native constraints, and passes a `delta.tables.DeltaTable` object to the transform. Delta support is **implemented;
release-gated**: ordinary PySpark 4.1.0 with Delta 4.1.0 has isolated live evidence, while the wider V11 admission
matrix is pending. See [Delta compatibility](../compatibility/DeltaTables.compat.md) before adopting it.

Import `Schema`, `Transform`, `input`, `step`, `transform`, and `StructureSession` from `structure`. Import the Delta
declarations, operations, `check`, and field factories from `structure.plugin.pyspark`.

## Declarations

| Structure API | Purpose | Example |
| --- | --- | --- |
| `delta_input(Schema)` | Caller-bound, read-only Delta relation | `current_orders = delta_input(OrderV1)` |
| `delta_output(Schema)` | Mutable Delta target and named result | `orders = delta_output(Order)` |
| `check(predicate, name=None)` | Expected native Delta CHECK | `check(status != "invalid", name="valid_status")` |

Declare CHECKs in `Schema.constraints`. Structure checks that the bound table has matching native CHECK metadata; it
does not create the table or install constraints. Table columns, types, and nullability must match the current declared
schema before mutation. For explicit evolution, the new output schema is checked after the commit. Every Delta binding
needs a native `DeltaTable`, not a DataFrame.

## Same-schema mutations

The target is a relation parameter in a `None`-returning effect step. A step may record several operations, which run
in source order. `@step` makes the input and output binding explicit when a schema is used by more than one relation.

```python
from structure import Schema, StructureSession, Transform, input, step, transform
from structure.plugin.pyspark import (
    check, delta_delete, delta_merge, delta_output, delta_update, string,
)


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)
    constraints = (check(status != "invalid", name="valid_status"),)


class Change(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


@transform
class Apply(Transform):
    changes = input(Change)
    orders = delta_output(Order)

    @step(input=(changes, orders), output=orders)
    def apply(self, change: Change, order: Order) -> None:
        delta_delete(order, where=order.id == "2")
        delta_update(order, where=order.id == "1", set=Order(status="pending"))
        (
            delta_merge(order, change, on=order.id == change.id)
            .when_matched_update(set=Order(status=change.status))
            .when_not_matched_insert(values=Order(id=change.id, status=change.status))
            .execute()
        )


# The caller has already created the table with the declared CHECK.
result = Apply(changes=changes_df, orders=table).run(StructureSession(spark=spark))
assert result.orders is table
```

`delta_delete(target, where=...)` and `delta_update(target, where=..., set=Schema(...))` require an explicit Boolean
predicate. `where=True` deliberately affects every row. Update values may specify some target fields; insert values
must supply every non-nullable target field. Predicates and assignments use typed Structure expressions, not SQL strings.

`delta_merge(target, source, on=...)` requires a target/source match expression. Its ordered builder supports
`when_matched_update`, `when_matched_delete`, `when_matched_update_all`, `when_not_matched_insert`,
`when_not_matched_insert_all`, `when_not_matched_by_source_update`, and `when_not_matched_by_source_delete`.
Clause methods accept `condition=` where Delta permits it; update and insert methods accept `set=` and `values=`
respectively. Finish the builder with `.execute()`. `delta_append(target, source).execute()` appends a source relation.

## Explicit schema evolution

For an expected schema change, declare the current table as `delta_input(CurrentSchema)` and the result as
`delta_output(NewSchema)`. The step's return annotation resolves that output. Return exactly one merge or append
operation with `.with_schema_evolution()`; do not pass the new output as a step parameter or invocation argument.

```python
from structure.plugin.pyspark import delta_input, delta_merge


class ChangeV2(Change):
    note = string()


class OrderV2(Order):
    note = string()


@transform
class EvolvingMerge(Transform):
    changes = input(ChangeV2)
    current_orders = delta_input(Order)
    orders = delta_output(OrderV2)

    def merge(self, change: ChangeV2, order: Order) -> OrderV2:
        return (
            delta_merge(order, change, on=order.id == change.id)
            .with_schema_evolution()
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


result = EvolvingMerge(changes=changes_v2_df, current_orders=table).run(StructureSession(spark=spark))
assert result.orders is table
```

Merge evolution maps to Delta's `withSchemaEvolution()`. The append form is
`return delta_append(order, change).with_schema_evolution().execute()`; it applies `mergeSchema=true` to that append
writer. Neither form changes a session-wide setting. Structure checks the old shape before the commit and the declared
new shape and CHECK metadata afterward. For a later invocation, bind the table using its new schema.

## CHECK comparison and runtime behavior

`delta_check_match` may be set in PySpark plugin configuration, `@transform(...)`, or `@step(...)`; the nearest setting
wins. The default, `"expression"`, compares native and declared CHECK predicates after normalizing supported SQL
syntax. `"name"` checks names without comparing predicates. `"off"` skips CHECK verification. Table shape checks
remain active in every mode. An unsupported native CHECK expression fails under the default mode; use `"name"` only
when name matching is sufficient for your table policy.

Delta operations are batch-only. Each operation makes its own native commit; Structure does not combine steps into a
transaction or retry uncertain commits. A later failure does not undo an earlier success. Online and generated modes
run the same checked operations. The returned `DeltaTable` is the caller's original object; if its `toDF()` was
materialized earlier, reopen the table to inspect the latest snapshot.

For rationale and lifecycle details, see [Delta table background](../background/DeltaTables.back.md). For the exact
release and target limits, see [Delta compatibility](../compatibility/DeltaTables.compat.md).
