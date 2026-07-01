# Sprint 07: Analytical Join Coverage

## Sprint Goal

Add compiler-visible v2 join forms for common analytical pipelines while preserving the strict v1 `join_one(...)`
contract.

## Product Outcome

Developers can express existence filters, row-multiplying joins, deterministic lookup dedupe, and time-aware lookup
joins in Structure source instead of hiding common join logic in hooks.

## Scope

### In Scope

- `exists(...)` predicate for semi join semantics.
- `not_exists(...)` predicate for anti join semantics.
- `join_many(...)` for intentional row multiplication.
- Deterministic `JoinDedupe.latest_by(...)` and `JoinDedupe.earliest_by(...)` policies.
- `temporal_one(...)` for SCD-style validity-window lookups.
- Backward `as_of_one(...)` with optional tolerance.
- Backend capability checks for every analytical join form.
- Shared PySpark recipes consumed by online and generated execution.
- Static traceability and `structure explain` output for analytical joins.
- Online/generated parity tests for every supported form.
- Streaming compatibility classification for v2 stream-static forms.

### Out of Scope

- Right joins.
- Full joins.
- Cross joins.
- Automatic join reordering.
- Cost-based optimization.
- Forward or nearest as-of joins.
- Stream-stream temporal joins.

## Relevant Specification Items

- As a developer, I can use existence joins so that semi and anti filters stay compiler-visible.
- As a developer, I can use `join_many(...)` so that row multiplication is explicit.
- As a developer, I can dedupe lookup inputs with deterministic policies so that selected right rows are reviewable.
- As a developer, I can model SCD-style temporal lookups with validity-window semantics.
- As a developer, I can model backward as-of lookups with optional tolerance.
- As a developer, diagnostics explain unsupported analytical join shapes and suggest hooks only as explicit escape
  hatches.

## Example Source

```python
def with_items(self, order: OrderNormalized) -> OrderItemFact:
    where(self.customers.exists(on=self.customers.id == order.customer_id))

    item = self.order_items.join_many(
        on=self.order_items.order_id == order.id,
        how=Join.INNER,
    )

    return OrderItemFact(
        order_id=order.id,
        item_id=item.id,
        amount=item.amount,
    )
```

## Engineering Tasks

1. + Add analytical join capability requirements for existence joins.
2. + Implement `exists(...)` and `not_exists(...)` symbolic predicates.
3. + Lower existence joins through shared PySpark recipes.
4. + Add no-Spark online/generated semantic-contract tests for existence joins.
5. Implement `join_many(...)` symbolic scope and IR.
6. Lower `join_many(...)` through shared PySpark recipes.
7. Add row multiplication parity tests.
8. Add deterministic dedupe policy objects and checks.
9. Add deduped `join_one(...)` recipe lowering and parity tests.
10. Implement `temporal_one(...)` closed-open validity-window lookups.
11. Implement backward `as_of_one(...)`.
12. Update traceability, explain output, diagnostics, and streaming classification.

## Progress

- [x] (2026-07-01) Implemented `exists(...)` and `not_exists(...)` as compiler-visible row-filtering join methods.
- [x] (2026-07-01) Lowered existence joins to default PySpark `left_semi` and `left_anti` recipes shared by generated
  rendering and online execution.
- [x] (2026-07-01) Added focused no-Spark coverage for IR metadata, generated rendering, online recipe execution,
  capability support, and the no-right-field-read guardrail.
- [x] (2026-07-01) Extended v2 model fixtures and examples with product-existence and blocked-product anti-existence
  filters.

## Acceptance Criteria

- Existence joins preserve current-row schema and filter rows correctly.
- Right-side duplicates do not change existence output.
- `join_many(...)` multiplies rows and records row-multiplying cardinality in traceability.
- Deduped lookup joins never use nondeterministic Spark row choice.
- Temporal joins use closed-open interval semantics and handle null `valid_to` as open-ended.
- Backward as-of joins select the latest right row at or before the left time.
- Unsupported analytical join capabilities fail with `BACKEND-E2402`.
- Online and generated execution produce equal output for each supported analytical join form.
- `structure explain` identifies row-filtering, row-multiplying, dedupe, temporal, and as-of joins clearly.

## Risks

- Temporal and as-of joins can look simple while hiding expensive windowing.
- Dedupe policies can become nondeterministic if tie handling is vague.
- Row-multiplying joins can surprise downstream schema assumptions.

## Notes

Ship existence joins and `join_many(...)` before temporal joins. They deliver high value with fewer runtime policy
questions, and they establish the cardinality model needed by the later time-aware forms.
