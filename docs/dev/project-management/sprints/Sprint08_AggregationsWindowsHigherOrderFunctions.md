# Sprint 08: Aggregations, Windows, and Higher-Order Functions

## Sprint Goal

Add the first broad analytical transform features: typed aggregations, window expressions, deterministic dedupe, and
compiler-visible array/map helpers.

## Product Outcome

Developers can write common analytical summaries, latest-row selections, rankings, lag/lead comparisons, rolling
metrics, and array/map transformations without dropping into hooks or hiding work from Spark's optimizer.

## Scope

### In Scope

- Typed `group_by(...)`.
- Aggregate expression builders for count, sum, min, max, average, and supported distinct counts.
- Aggregate output schema checks.
- Window specification objects with partitioning, ordering, and frames.
- Ranking, lag, lead, rolling metric, latest-row, earliest-row, and duplicate-removal helpers.
- Deterministic tie policies for selected-row dedupe.
- Spark higher-order helper forms for arrays and maps where supported by the configured PySpark target.
- Shared PySpark recipes consumed by online and generated execution.
- Online/generated parity tests and generated-code snapshots for each admitted feature family.

### Out of Scope

- Analytical join forms covered by Sprint 07.
- Production incremental compile.
- Generated documentation artifacts.
- Automatic query optimization.
- Arbitrary Python callbacks inside higher-order helpers.

## Relevant Specification Items

- As a developer, I can define typed aggregation subtransforms.
- As a developer, I can group by one or more typed fields.
- As a developer, I can calculate common aggregate metrics.
- As a developer, I can receive type and nullability diagnostics for aggregate expressions.
- As a developer, I can define window expressions for ranking, dedupe, latest-row selection, and rolling metrics.
- As a developer, I can define lag and lead expressions.
- As a developer, I can select latest or earliest rows with deterministic tie policy.
- As a developer, I can use higher-order function helpers for arrays and maps.

## Example Source

```python
def summarize(self, order: OrderEnriched) -> CustomerSummary:
    return (
        group_by(order.customer_id)
        .agg(
            order_count=count(),
            total_revenue=sum_(order.total),
            last_order_at=max_(order.order_time),
        )
        .as_schema(CustomerSummary)
    )
```

## Engineering Tasks

1. Implement typed `group_by(...)` capture and aggregate IR.
2. Implement aggregate expression builders and type checks.
3. Lower aggregate plans through shared PySpark recipes.
4. Add aggregate generated-code snapshots and online/generated parity tests.
5. Implement window specification objects and window IR.
6. Implement ranking, lag, lead, rolling metric, latest-row, and earliest-row helpers.
7. Implement duplicate-removal helpers with deterministic selected-row tie policies.
8. Implement supported array and map higher-order helpers.
9. Add unsupported callback diagnostics for higher-order helpers.
10. Update traceability, explain output, and streaming compatibility classification for admitted features.

## Acceptance Criteria

- Grouped aggregate transforms compile and run online and generated.
- Aggregate output schema fields are validated against grouped keys and aggregate expression types.
- Window helpers compile to Spark window operations without UDFs.
- Latest-row and earliest-row helpers reject ambiguous tie behavior unless a policy is explicit.
- Higher-order helper callbacks are symbolic and Spark-plan-visible.
- Unsupported helper callbacks produce actionable diagnostics.
- Online and generated execution produce equal output for each admitted feature form.

## Progress

- [ ] Implement aggregation source capture, IR, recipes, generated snapshots, and parity tests.
- [ ] Implement window and dedupe helpers with deterministic policies.
- [ ] Implement supported higher-order array and map helpers.
- [ ] Update explain, traceability, diagnostics, and streaming compatibility classification.

## Compile-Time Performance Metric

Track compile time and generated file count for aggregate-heavy and window-heavy fixtures.

Targets:

- 10 aggregate transforms compile in under 3 seconds excluding Spark startup.
- 10 window transforms compile in under 3 seconds excluding Spark startup.

## Risks

- Aggregation syntax can become awkward if it fights the schema-returning method model.
- Window helpers can hide expensive plans if explain output does not show partition and ordering clearly.
- Higher-order helper callbacks can look like ordinary Python even when only symbolic expressions are supported.

## Notes

Ship simple grouped rollups before advanced grouping sets. Ship deterministic latest-row helpers before broad dedupe
shortcuts so the public API defaults to explainable behavior.
