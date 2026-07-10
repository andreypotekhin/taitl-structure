# Analytical Join Coverage

Analytical joins are join forms that go beyond one-row lookup enrichment. They let a transform filter rows by
existence, intentionally multiply rows by matches, select deterministic lookup winners, and join against time-valid
records.

This reference describes Structure's analytical join family: semi and anti existence filters, `inner_join(...)`,
deterministic lookup dedupe, temporal lookups, as-of lookups, and slowly changing dimension lookups.

The `lookup_join(...)` contract remains unchanged. It is a narrow many-to-one or one-to-one lookup join. It warns when
right-side uniqueness is not proven and never deduplicates by surprise.

## Scope

This reference covers source semantics for analytical joins. Existence joins, `inner_join(...)`, deterministic
deduped `lookup_join(...)`, temporal validity-window `temporal_one(...)`, and backward `as_of_one(...)` are implemented in
the default PySpark profile.
[JoinSemantics.md](JoinSemantics.md) covers the strict `lookup_join(...)` contract.

In scope for the analytical join family:

- existence predicates that lower to semi and anti joins;
- row-multiplying `inner_join(...)`;
- deterministic right-side dedupe before `lookup_join(...)`;
- temporal validity-window lookup joins;
- as-of lookup joins;
- diagnostics, IR, backend capability requirements, and traceability for those forms.

Out of scope for this first analytical join slice:

- right joins;
- full joins;
- cross joins;
- automatic join reordering;
- cost-based optimization;
- stream-stream temporal joins;
- storage write behavior after row-multiplying joins.

Right, full, cross, non-equi, and disjunctive rowset joins are covered in
[FullPySparkJoinSupport.md](FullPySparkJoinSupport.md).

## Terms

A current row is the row flowing through the source-ordered transform chain.

A right input is a named input referenced through an input scope, such as typed parameter `customer` or class input
scope `self.customers`.

A row-preserving join keeps every current row. A left `lookup_join(...)` is row-preserving.

A row-filtering join keeps or removes current rows based on match existence. Semi and anti joins are row-filtering.

A row-multiplying join can produce more than one output row for one current row. `inner_join(...)` is row-multiplying.

A select-one join chooses at most one right row per current row by uniqueness, dedupe policy, temporal validity, or
as-of ordering.

## Existence Joins

Existence joins should use free predicate functions:

```python
where(exists(on=customer.id == order.customer_id))
where(not_exists(on=suppressed_email.email == order.email))
```

`exists(...)` keeps current rows that have at least one right match. It has semi join semantics.

`not_exists(...)` keeps current rows that have no right match. It has anti join semantics.

Rules:

- The method returns a symbolic boolean predicate.
- No right-side fields are exposed.
- Right-side duplicates do not change the result.
- The `on` condition follows the same equi-join condition rules as `lookup_join(...)`.
- Normal equality and null-safe equality keep the same meaning as in `JoinSemantics.md`.
- The predicate may appear in `where(...)` or in boolean combinations when the expression remains compileable.

Generated PySpark may lower these forms to left semi and left anti joins, or to an equivalent plan, as long as row
order, row count, schema, null semantics, and diagnostics remain equivalent.

## `inner_join(...)`

`inner_join(...)` intentionally admits row multiplication:

```python
inner_join(
    on=order_item.order_id == order.id,
    how=Join.INNER,
)
```

Rules:

- `Join.INNER` keeps only current rows with at least one right match.
- `Join.LEFT` keeps unmatched current rows with null right fields.
- Duplicate right rows are allowed and expected.
- No uniqueness warning is emitted.
- The joined scope exposes right-side fields.
- Output schema construction controls final fields; Structure must not append right-side fields implicitly.
- Right-side projection should carry only keys and referenced fields.

`inner_join(...)` should be used when the business output is one row per match, such as order-to-line-item expansion.

## Deterministic Lookup Dedupe

Some lookup inputs contain multiple right rows per key, but the desired business rule is still one selected right row.
That rule must be explicit:

```python
lookup_join(
    on=self.customer_snapshots.id == order.customer_id,
    how=Join.LEFT,
    dedupe=JoinDedupe.latest_by(
        self.customer_snapshots.updated_at,
        ties=TiePolicy.ERROR,
    ),
)
```

Rules:

- Dedupe policies reduce the right input before the lookup join.
- A dedupe policy must name the ordering or selection rule.
- The default tie policy is `TiePolicy.ERROR`.
- Structure must not lower dedupe to arbitrary `first(...)` or nondeterministic `dropDuplicates(...)`.
- A deduped `lookup_join(...)` records both the original right input and the deduped lookup dependency in traceability.
- Runtime tie checks are explicit because they can add Spark work.
- Current PySpark lowering uses `row_number()` over a window partitioned by the right-side join keys, ordered by the
  explicit policy expression, keeps rank `1`, drops the temporary rank column, and then applies the lookup join.

Initial policy family:

- `JoinDedupe.latest_by(order_by, ties=TiePolicy.ERROR)`;
- `JoinDedupe.earliest_by(order_by, ties=TiePolicy.ERROR)`;
- composite ordering by passing ordered expressions once the expression model supports it.

## Temporal Validity Lookups

Temporal validity lookups select a right row whose validity window contains a current-row event time:

```python
temporal_one(
    on=self.customer_history.id == order.customer_id,
    at=order.order_time,
    valid_from=self.customer_history.valid_from,
    valid_to=self.customer_history.valid_to,
    how=Join.LEFT,
    overlaps=OverlapPolicy.ERROR,
)
```

Default interval semantics are closed-open:

```text
valid_from <= at < valid_to
```

A null `valid_to` means the right row is open-ended and current after `valid_from`.

Rules:

- `on` supplies equality key pairs.
- `at` is a timestamp or date expression from the current row or an earlier joined scope.
- `valid_from` and `valid_to` come from the right input.
- Overlapping windows for the same right key are invalid for `temporal_one(...)`.
- Overlap checks are explicit runtime checks unless uniqueness and non-overlap can be proven from metadata.
- `Join.LEFT` and `Join.INNER` are the initial supported join types.
- Temporal fields participate in traceability and diagnostics.

This form is the Structure model for SCD type 2 lookup joins. It should not assume any table format or storage
convention.

## As-Of Lookups

As-of lookups select the nearest right-side record relative to a current-row time:

```python
price = self.prices.as_of_one(
    on=self.prices.symbol == trade.symbol,
    left_time=trade.trade_time,
    right_time=self.prices.price_time,
    direction=AsOf.BACKWARD,
    tolerance=duration("1 day"),
    how=Join.LEFT,
)
```

Initial rules:

- `AsOf.BACKWARD` chooses the latest right row whose `right_time <= left_time`.
- `tolerance` is optional and rejects matches farther away than the supplied duration.
- `Join.LEFT` keeps unmatched rows with null right fields.
- `Join.INNER` removes unmatched rows.
- Ties on `right_time` require an explicit tie policy.
- Forward and nearest-direction as-of joins are deferred until backward joins are stable.

## IR Contract

Analytical joins extend `JoinOperation`.

Required fields:

- method;
- cardinality;
- current scope;
- right input scope;
- occurrence id;
- ordered key pairs;
- equality kind per key pair;
- join type where applicable;
- hint where applicable;
- referenced right fields;
- dedupe policy;
- temporal policy;
- as-of policy;
- tie policy;
- overlap policy;
- runtime check requirements;
- source location and source expression text.

Allowed semantic method values begin with:

- `exists`;
- `not_exists`;
- `inner_join`;
- `lookup_join` with a dedupe policy;
- `temporal_one`;
- `as_of_one`.

The exact enum names may follow the implementation's local naming style, but the semantic categories must remain
separate.

## Backend Capabilities

Each analytical join form requires a backend capability before lowering:

```text
join.exists
join.not_exists
join.inner_join
join.lookup_dedupe
join.temporal_one
join.as_of_one
join.rowset_join
```

The default PySpark profile supports `join.exists`, `join.not_exists`, `join.inner_join`, `join.lookup_dedupe`,
`join.temporal_one`, `join.as_of_one`, and `join.rowset_join`.
Unsupported capability diagnostics use `BACKEND-E2402` and link to this reference. The diagnostic names the
join form and suggest either a supported join, a hook escape hatch, or waiting for the planned feature.

Right, full, cross, non-equi, and disjunctive rowset joins are covered in
[FullPySparkJoinSupport.md](FullPySparkJoinSupport.md).

## Streaming Compatibility

v.2 may classify stream-static `exists(...)`, `not_exists(...)`, and `inner_join(...)` as compatible when the current
pipeline input is streaming and the right input is static, if Spark supports the lowered plan for the configured
target.

Temporal, deduped lookup, as-of lookup, and runtime tie or overlap checks are batch-only until a streaming-specific
design specifies their state, watermark, and output-mode requirements.

## Diagnostics

Example:

```text
CompileError JOIN-E2701: inner_join(...) cannot feed a one-row-only output assumption

Join:
  EnrichOrders.add_items -> order_items#1

Cardinality:
  row-multiplying

Problem:
  The downstream schema construction assumes one output row per current row, but inner_join(...) may produce many rows.

Use:
  return a schema that represents one row per item, aggregate before this step, or use lookup_join(...) when the right key
  is unique.

See docs/reference/AnalyticalJoinCoverage.md
```
