# Sprint 05: Joins, Compiler Lineage, and Build Integration

## Sprint Goal

Add symbolic `join_one(...)`, support arbitrary N-step serial joins, add compiler provenance and static dataflow
lineage, and make generated code reliable in CI with `--fail-on-diff`.

## Product Outcome

Developers can build practical enrichment pipelines with multiple inputs and source-ordered typed subtransforms, then
verify generated code is committed and explain how source code maps to IR, generated PySpark, and static data
dependencies.

## Scope

### In Scope

- Named input scopes in symbolic execution.
- `join_one(...)`.
- Join type enum.
- Join hint enum.
- Predictable DataFrame aliasing.
- Serial N-step joins.
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow lineage inferred from IR.
- `structure explain`.
- `structure compile --fail-on-diff`.
- Streaming compatibility static checks for supported v1 operations.

### Out of Scope

- `join_many(...)`; row-multiplying joins are v2 work.
- Aggregations and windowing.
- Runtime LDJSON lineage.
- Full streaming orchestration.

## Relevant Specification Items

- As a developer, I can express symbolic `join_one(...)` joins.
- As a developer, I can build serial joins across arbitrary numbers of named inputs.
- As a developer, I can specify join type with enums.
- As a developer, I can specify join hints with enums.
- As a developer, I can inspect compiler provenance from source node to IR node to generated PySpark node.
- As a developer, I can inspect static dataflow lineage for transform, table, and column dependencies inferred from IR.
- As a developer, I can run `structure compile --fail-on-diff` in CI.
- As a developer, I can run `structure explain` to see transform structure.
- As a developer, generated transforms remain streaming-compatible when Spark supports the operations used.

## Example Source

```python
def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
    customer = self.customers.join_one(
        on=self.customers.id == order.customer_id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer(
        id=order.id,
        customer_name=customer.name,
        total=order.total,
    )
```

## Example Generated PySpark

```python
df = df.alias("order_normalized")
customers_df = F.broadcast(customers.alias("customers"))

    df = df.join(
        customers_df,
        F.col("customers.id") == F.col("order_normalized.customer_id"),
        "left",
    )

    df = df.select(
        F.col("order_normalized.id").alias("id"),
        F.col("customers.name").alias("customer_name"),
        F.col("order_normalized.total").alias("total"),
    )
```

## Engineering Tasks

1. Implement named input symbolic scopes.
2. Implement `join_one(...)` symbolic operation.
3. Implement join enums.
4. Implement join IR.
5. Generate PySpark joins.
6. Generate predictable aliases.
7. Add N-step serial join fixture with more than three inputs.
8. Add compiler provenance records.
9. Infer static dataflow lineage from IR.
10. Implement `structure explain`.
11. Implement `--fail-on-diff`.
12. Add streaming compatibility check stubs.

## Acceptance Criteria

- Single lookup join compiles and runs.
- Serial join with at least five named inputs compiles and runs.
- Diagnostics can show source node, IR node, and generated PySpark node for supported compiler errors.
- `structure explain` shows transform, table, and column dependencies inferred from IR.
- `--fail-on-diff` fails when generated code differs.
- `structure explain` prints a useful step summary.
- Generated compiled path remains UDF-free.

## Compile-Time Performance Metric

Track compile time for N-step join fixtures.

Targets:

- 5-input serial join fixture compiles in under 2 seconds excluding Spark startup.
- Synthetic 25-step join fixture exposes no obvious quadratic behavior.

## Risks

- Alias management can become error-prone.
- Join cardinality semantics may be under-specified.
- Lineage can become noisy if column-level details are emitted by default.

## Notes

Keep the default lineage explanation compact. Column-level details can be an opt-in `structure explain` view. Runtime
LDJSON lineage is deferred beyond v4 in `docs/dev/project-management/NiceToHave.md`.
