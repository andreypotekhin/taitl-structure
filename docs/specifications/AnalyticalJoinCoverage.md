# Analytical Join Coverage

## Purpose

Analytical pipelines need more than lookup joins. This specification defines the staged v2+ join family that resolves
C27 from [Challenges.md](../dev/design/Challenges.md): semi and anti existence filters, `join_many(...)`, deterministic lookup
dedupe, temporal lookups, as-of lookups, and slowly changing dimension lookups.

The v1 `join_one(...)` contract remains unchanged. It is a narrow many-to-one or one-to-one lookup join. It warns when
right-side uniqueness is not proven and never deduplicates by surprise.

## Scope

This specification owns source semantics for analytical joins. Existence joins, `join_many(...)`, and deterministic
deduped `join_one(...)` are implemented in the default PySpark profile. Temporal and as-of joins remain staged.
[JoinSemantics.md](JoinSemantics.md) remains the authority for the strict v1 `join_one(...)` contract.

In scope for the analytical join family:

- existence predicates that lower to semi and anti joins;
- row-multiplying `join_many(...)`;
- deterministic right-side dedupe before `join_one(...)`;
- temporal validity-window lookup joins;
- as-of lookup joins;
- diagnostics, IR, backend capability requirements, traceability, and tests for those forms.

Out of scope until a later design:

- right joins;
- full joins;
- cross joins;
- automatic join reordering;
- cost-based optimization;
- stream-stream temporal joins;
- storage write behavior after row-multiplying joins.

## Terms

A current row is the row flowing through the source-ordered transform chain.

A right input is a named input referenced through an input scope, such as `self.customers`.

A row-preserving join keeps every current row. A left `join_one(...)` is row-preserving.

A row-filtering join keeps or removes current rows based on match existence. Semi and anti joins are row-filtering.

A row-multiplying join can produce more than one output row for one current row. `join_many(...)` is row-multiplying.

A select-one join chooses at most one right row per current row by uniqueness, dedupe policy, temporal validity, or
as-of ordering.

## Existence Joins

Existence joins should use predicate methods on right input scopes:

```python
where(self.customers.exists(on=self.customers.id == order.customer_id))
where(self.suppressed_emails.not_exists(on=self.suppressed_emails.email == order.email))
```

`exists(...)` keeps current rows that have at least one right match. It has semi join semantics.

`not_exists(...)` keeps current rows that have no right match. It has anti join semantics.

Rules:

- The method returns a symbolic boolean predicate.
- No right-side fields are exposed.
- Right-side duplicates do not change the result.
- The `on` condition follows the same equi-join condition rules as `join_one(...)`.
- Normal equality and null-safe equality keep the same meaning as in `JoinSemantics.md`.
- The predicate may appear in `where(...)` or in boolean combinations when the expression remains compileable.

Generated PySpark may lower these forms to left semi and left anti joins, or to an equivalent plan, as long as row
order, row count, schema, null semantics, and diagnostics remain equivalent.

## `join_many(...)`

`join_many(...)` intentionally admits row multiplication:

```python
item = self.order_items.join_many(
    on=self.order_items.order_id == order.id,
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

`join_many(...)` should be used when the business output is one row per match, such as order-to-line-item expansion.

## Deterministic Lookup Dedupe

Some lookup inputs contain multiple right rows per key, but the desired business rule is still one selected right row.
That rule must be explicit:

```python
customer = join_one(
    self.customer_snapshots,
    on=self.customer_snapshots.id == order.customer_id,
    how=Join.LEFT,
    dedupe=JoinDedupe.latest_by(
        order_by=self.customer_snapshots.updated_at,
        ties=TiePolicy.ERROR,
    ),
)
```

Rules:

- Dedupe policies reduce the right input before the lookup join.
- A dedupe policy must name the ordering or selection rule.
- The default tie policy is `TiePolicy.ERROR`.
- Structure must not lower dedupe to arbitrary `first(...)` or nondeterministic `dropDuplicates(...)`.
- A deduped `join_one(...)` records both the original right input and the deduped lookup dependency in traceability.
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
customer = self.customer_history.temporal_one(
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
- `join_many`;
- `join_one` with a dedupe policy;
- `temporal_one`;
- `as_of_one`.

The exact enum names may follow the implementation's local naming style, but the semantic categories must remain
separate.

## Backend Capabilities

Each analytical join form requires a backend capability before lowering:

```text
join.exists
join.not_exists
join.join_many
join.lookup_dedupe
join.temporal_one
join.as_of_one
```

The default PySpark profile supports `join.exists`, `join.not_exists`, `join.join_many`, and `join.lookup_dedupe`.
Unsupported capability diagnostics use `BACKEND-E2402` and link to this specification. The diagnostic must name the
join form and suggest either a supported join, a hook escape hatch, or waiting for the planned feature.

## Streaming Compatibility

v2 may classify stream-static `exists(...)`, `not_exists(...)`, and `join_many(...)` as compatible when the current
pipeline input is streaming and the right input is static, if Spark supports the lowered plan for the configured
target.

Temporal, as-of, deduped lookup, and runtime tie or overlap checks are batch-only until a streaming-specific design
specifies their state, watermark, and output-mode requirements.

## Diagnostics

Diagnostics must include:

- transform and subtransform;
- join method;
- right input;
- source condition;
- cardinality classification;
- key pairs;
- dedupe, temporal, as-of, tie, or overlap policy when present;
- runtime check cost when present;
- suggested source fix;
- link to this specification.

Example:

```text
CompileError JOIN-E2701: join_many(...) cannot feed a one-row-only output assumption

Join:
  EnrichOrders.add_items -> order_items#1

Cardinality:
  row-multiplying

Problem:
  The downstream schema construction assumes one output row per current row, but join_many(...) may produce many rows.

Use:
  return a schema that represents one row per item, aggregate before this step, or use join_one(...) when the right key
  is unique.

See docs/specifications/AnalyticalJoinCoverage.md
```

## Acceptance Scenarios

The analytical join family is implemented incrementally. Each admitted join form requires tests proving:

- source DSL capture;
- backend capability acceptance and rejection;
- IR shape and cardinality classification;
- generated PySpark rendering;
- online/generated parity on small DataFrames;
- static traceability output;
- diagnostics for unsupported shapes;
- streaming compatibility classification where applicable.

Specific scenarios:

- `exists(...)` keeps only current rows with a matching right key.
- `not_exists(...)` keeps only current rows without a matching right key.
- Right-side duplicates do not change existence join output.
- `join_many(...)` multiplies rows when multiple right rows match.
- Left `join_many(...)` preserves unmatched current rows.
- Deduped `join_one(...)` selects the latest right row by an explicit order expression.
- Deduped `join_one(...)` does not emit the ordinary unproven-uniqueness warning because the right side is reduced by
  policy before the lookup.
- Runtime tie diagnostics are follow-up work for `TiePolicy.ERROR`.
- `temporal_one(...)` matches a right row whose validity window contains the event time.
- Overlapping temporal windows are diagnosed when overlap checks are enabled.
- Backward `as_of_one(...)` selects the latest right row at or before the current-row time.
- As-of tolerance rejects matches outside the allowed time distance.
