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
- Aggregate expression builders for count, sum, min, max, avg, and supported distinct counts.
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

- [x] (2026-07-02) Started the aggregation slice: implemented typed `group_by(...)` capture, aggregate IR, count/sum
  builders, shared PySpark aggregate recipes, generated rendering coverage, online recipe coverage, and v2 analytics
  fixture lowering.
- [x] (2026-07-02) Extended aggregate builders and PySpark lowering to `min(...)`, `max(...)`, `avg(...)`, and
  `count_distinct(...)`, with generated rendering, online recipe, capability, and v2 fixture coverage.
- [x] (2026-07-02) Added aggregate input type and nullable-output diagnostics for numeric aggregate expressions.
- [x] (2026-07-02) Added aggregate static dataflow traceability and explain output for grouped keys and aggregate
  metrics.
- [x] (2026-07-02) Classified grouped aggregates as batch-only in streaming compatibility reports and explain output.
- [x] (2026-07-02) Implemented Spark-visible array `arr_transform(...)` and `arr_filter(...)` callbacks with generated
  rendering and online expression coverage.
- [x] (2026-07-02) Added array higher-order helper diagnostics for non-array inputs, non-Boolean filters, arbitrary
  Python boolean callback flow, and untyped callback returns.
- [x] (2026-07-02) Updated QuickRef, example order/analytics models, generated example artifacts, and generation
  stability tests to cover admitted grouped aggregates and array higher-order helpers.
- [x] (2026-07-02) Implemented Spark-visible map `map_transform_values(...)` and `map_filter(...)` callbacks with
  generated rendering, online expression coverage, public API snapshots, docs, and example fixture coverage.
- [x] (2026-07-02) Implemented `latest_by(...)` and `earliest_by(...)` selected-row helpers with selected-row IR,
  shared PySpark recipes, generated and online `row_number()` lowering, explain output, traceability, streaming
  batch-only classification, backend capability coverage, and public docs.
- [x] (2026-07-03) Implemented exact current-frame `distinct()` / `drop_duplicates()` cleanup with IR capability,
  shared PySpark operation recipes, generated and online `dropDuplicates()` lowering, explain output, traceability,
  streaming batch-only classification, backend capability coverage, and public docs.
- [x] (2026-07-03) Extended `drop_duplicates(...)` with PySpark-compatible typed field subsets for convenience, while
  keeping `distinct()` exact-only and documenting the representative-row tradeoff.
- [ ] Implement aggregation source capture, IR, recipes, generated snapshots, and parity tests.
- [ ] Implement broad window and deterministic keyed dedupe helpers.
- [x] Implement supported higher-order array and map helpers.
- [ ] Finish explain, traceability, diagnostics, and streaming compatibility classification for remaining Sprint 08
  feature families.

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

Simple grouped rollups, higher-order collection helpers, deterministic latest/earliest row selection, and exact
current-frame duplicate cleanup are the admitted v2 analytical slice. Keep keyed dedupe shortcuts behind explicit
specs so the public API defaults to explainable behavior.
