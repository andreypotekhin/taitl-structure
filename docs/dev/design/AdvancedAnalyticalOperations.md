# Design: Advanced Analytical Operations

## Purpose

Sprint 08 admitted the first practical analytical slice: grouped aggregates, selected-row helpers, exact/subset
dedupe, projection windows, rolling row metrics, and basic array/map higher-order helpers. This design defines the
remaining aggregation, window, and higher-order function surface needed for full v2 analytical support.

The goal is not to mirror every PySpark function. The goal is to admit the analytical operations that are common,
typed, reviewable, explainable, and reusable across execution and generated-code execution without hidden UDFs.

## Design Stance

Advanced analytical operations must stay compiler-visible. A helper is admitted only when Structure can represent it
in IR, validate type and nullability, classify backend capability, explain field lineage, and lower execution and
generated PySpark through the shared recipe layer.

The public DSL should expose business-level intent:

- aggregation groups and metrics;
- named multi-level summaries;
- deterministic row selection;
- reusable window specifications;
- explicit window frames;
- symbolic collection lambdas.

The DSL should not become a second spelling of the whole Spark API. Rare, target-specific, or ambiguous operations
remain hooks until they have a Structure-level contract.

## First Slice Boundary

Already admitted in Sprint 08:

- `group_by(...)` with one or more grouping keys;
- `count()`, `count_distinct(...)`, `sum(...)`, `min(...)`, `max(...)`, and `avg(...)`;
- `latest_by(...)`, `earliest_by(...)`, `dedupe_latest_by(...)`, and `dedupe_earliest_by(...)`;
- `row_number(...)`, `rank(...)`, `dense_rank(...)`, `lag(...)`, `lead(...)`;
- `rolling_sum(...)`, `rolling_avg(...)`, `rolling_min(...)`, and `rolling_max(...)`;
- `distinct()` and `drop_duplicates(...)`;
- `arr_transform(...)`, `arr_filter(...)`, `map_transform_values(...)`, and `map_filter(...)`.

This design covers what remains after that slice.

## Aggregation Design

Full aggregation support adds advanced grouping shapes, more metric families, and post-aggregate filtering.

Public grouping helpers should include:

- `rollup(...)` for hierarchical totals;
- `cube(...)` for all combinations of selected grouping keys;
- `grouping_sets(...)` for named explicit grouping levels;
- a grand-total grouping set by passing an empty grouping level;
- `grouping_id()` and `is_grouped(field)` helpers when a schema needs to distinguish subtotal rows.

Advanced aggregate helpers should include:

- exact metrics: `first_value(...)`, `last_value(...)`, `bool_and(...)`, `bool_or(...)`;
- statistical metrics: `stddev(...)`, `variance(...)`, `corr(...)`, and `covar(...)`;
- approximate metrics: `approx_count_distinct(...)` and `approx_percentile(...)`;
- collection metrics: `collect_list(...)` and `collect_set(...)` only with explicit output element type and
  cardinality warning;
- filtered metrics using `where=...`, such as `sum(order.total, where=order.status == "paid")`.

Rules:

- Grouping key aliases are required when the key expression is not a direct field reference.
- Grouping-set outputs must model subtotal rows explicitly: omitted grouping keys are nullable unless the user fills
  them with a literal label.
- `first_value(...)` and `last_value(...)` are valid only with explicit ordering, because unordered first/last is not
  deterministic.
- Collection metrics must warn that result array ordering is not guaranteed unless an explicit ordered aggregate form
  is later admitted.
- Filtered aggregate predicates may reference the pre-aggregate row scope only.
- Post-aggregate filtering should be represented as `having(...)`, not as a pre-aggregate `where(...)`.

## Window Design

Full window support needs reusable window specs and explicit frames. Projection helpers should accept either inline
`partition_by` and `order_by` arguments or a named `window(...)` object.

Public helpers should include:

- `window(partition_by=..., order_by=..., frame=...)`;
- `rows_between(start, end)` and `range_between(start, end)`;
- `unbounded_preceding()`, `unbounded_following()`, `current_row()`, `preceding(n)`, and `following(n)`;
- ranking helpers `percent_rank()`, `cume_dist()`, and `ntile(n)`;
- value helpers `first_value(...)`, `last_value(...)`, and `nth_value(...)`;
- window aggregate helpers `window_sum(...)`, `window_avg(...)`, `window_min(...)`, `window_max(...)`,
  `window_count(...)`, and `window_count_distinct(...)` where supported.

Rules:

- Window specs are immutable value objects captured during symbolic execution.
- Ranking and offset windows require `order_by`.
- Aggregate windows require a frame; default frames are not inferred for broad helpers.
- Range frames require one order expression with a numeric, date, timestamp, or interval-compatible type.
- Multi-column ordering supports explicit direction and null ordering per key.
- Offset and value helpers may opt into `ignore_nulls=True` only when the backend supports it.
- Any helper that can choose among tied rows must expose the tie policy or require a complete deterministic order.

Streaming support remains deferred. Broad windows are batch-only until Structure defines watermark, state, output-mode,
and late-data semantics.

## Higher-Order Function Design

Full HOF support expands collection helpers while keeping callbacks symbolic. A callback is a one-time function over
Structure expression placeholders, not row-wise Python.

Public array helpers should include:

- `arr_exists(...)`;
- `arr_forall(...)`;
- `arr_zip_with(...)`;
- `arr_aggregate(...)`;
- `arr_sort_by(...)` for deterministic sortable keys where the backend supports comparator or key extraction;
- `arr_flatten(...)`, `arr_distinct(...)`, and `arr_position(...)` as non-callback collection expressions.

Public map helpers should include:

- `map_transform_keys(...)`;
- `map_zip_with(...)`;
- `map_keys(...)`;
- `map_values(...)`;
- `map_entries(...)`;
- `map_from_entries(...)`.

Rules:

- Callback arity is checked from the helper: array element, array element plus index where admitted, map key/value, or
  accumulator/value for aggregation.
- Callback returns must be typed Structure expressions or typed literals.
- Python `if`, `and`, `or`, loops, mutation, list/dict construction with symbolic values, and side effects are
  rejected inside callbacks unless a later symbolic form admits them explicitly.
- Lambdas may close over literals, enum values, and `@special(type="expr")` helpers, but not live DataFrames, Spark columns,
  sessions, mutable containers, or runtime-only objects.
- Nested HOFs are admitted only when the inner lambda does not capture an outer placeholder in a way the target cannot
  lower.
- Map key transforms must reject duplicate-key ambiguity unless the helper names a merge policy.

## IR Model

The IR should add explicit analytical variants instead of overloading generic call expressions:

```text
AggregateOperation
  grouping
  metrics
  having

Grouping
  kind: group_by | rollup | cube | grouping_sets
  levels
  keys

WindowSpecExpr
  partition_by
  order_by
  frame

WindowExpr
  function
  value
  spec
  options

HigherOrderExpr
  helper
  collection
  placeholders
  body
  options
```

These nodes feed backend capability checks and explain output before PySpark-specific recipes are selected.

## Diagnostics

Diagnostics must point users to public reference docs and say how to fix the source:

- missing alias for expression grouping key;
- grouping-set output field cannot represent subtotal nulls;
- aggregate helper used with a non-numeric or non-orderable input;
- unordered first/last aggregate;
- `having(...)` predicate references a field that is not grouped or aggregated;
- range frame uses an unsupported order type;
- window helper can choose a tied row without deterministic ordering;
- HOF callback uses Python control flow, returns an untyped value, or captures runtime state;
- map key transform can create duplicate keys without a merge policy.

## Explain and Traceability

Explain output should keep compact mode readable:

- `aggregate(grouping_sets, metrics=5, cardinality=aggregate)`;
- `window(percent_rank, partitions=2, order=2, frame=rows)`;
- `hof(arr_zip_with, callback=symbolic)`.

Expanded explain should show field lineage:

- each grouping key and subtotal level;
- each aggregate metric, filter, input fields, and output field;
- each window partition/order/frame dependency;
- HOF callback input placeholders and output expression lineage.

## Deferred

The following stay out until later designs:

- streaming windows, streaming aggregations, watermarks, triggers, and state policies;
- automatic optimization or query-plan rewriting;
- arbitrary Python callbacks or UDF fallback;
- ordered collection aggregation guarantees beyond explicitly ordered helper forms;
- target-specific helpers that have no backend-neutral meaning.

## Explicit Optimization Controls

The optimization slice adds compiler-visible controls without creating a second PySpark API. Each control exposes the
smallest Structure-level concept needed by users, represents it in IR, and lowers through the shared target recipes.
Operations that are rare or backend-specific remain explicit hooks. Unsupported operations fail before rendering or
execution instead of silently becoming UDFs, RDD work, or row-wise Python callbacks.

### Higher-Order Functions

Array and map callbacks are symbolic callbacks over Structure expressions. They lower to Spark SQL higher-order
functions or equivalent PySpark expressions and must not accept arbitrary Python callbacks that require row-wise Python
execution or hidden UDF generation.

### Caching and Persistence

Caching is explicit at step-method boundaries. A cache annotation may lower to a corresponding `persist(...)` call with
the selected storage level; Structure must never cache implicitly.

### Join Strategies

Join hints may name a strategy such as `auto`, `broadcast`, `shuffle_hash`, `sort_merge`, or
`shuffle_replicate_nl`. The hint is part of the checked join recipe and remains visible in generated code and explain
output.

### Aggregation and Performance Guardrails

Grouping sets, rollups, cubes, approximate aggregates, and filtered aggregates are modeled as Structure semantics first.
The target may lower them through `groupBy(...).agg(...)`, window expressions, or compatible target syntax, but the
public DSL remains smaller than the full target API. Every optimization feature must remain explicit and Spark-plan
visible; it must never hide Python UDFs or row-wise execution.

## Typed Binary, Parsing, and Grouped Mode Families

The deferred PySpark families are admitted only through typed contracts. Binary values use a public immutable Binary
field type, and base64/charset encoding helpers retain explicit input and output types. Schema-carrying JSON and CSV
conversion requires a declared output Schema and normalized literal options; it does not infer schemas or accept a free-
form options dictionary. Grouped `mode(value, deterministic=False)` follows the ordinary aggregate placement rules,
with deterministic ties lowered through a portable typed Spark expression for the shared PySpark target range.

These families share the same design constraints as the analytical helpers: capability checks, online/generated parity,
traceability, explain output, and target evidence are required before a catalog row is supported. Malformed parsing,
invalid binary data, non-orderable deterministic mode values, streaming mode, and other target disagreements remain
explicitly capability-gated or rejected.
