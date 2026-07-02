# Design: Analytical Join Coverage

## Purpose

C27 is resolved by treating analytical joins as a staged feature family instead of stretching v1 `join_one(...)`.
Structure v1 keeps lookup joins narrow and predictable. v2 adds compiler-visible syntax for the common analytical
join shapes that otherwise force users into opaque hooks: existence filters, row-multiplying joins, deterministic lookup
dedupe, temporal lookups, and slowly changing dimension lookups.

## Design Boundary

`join_one(...)` remains the v1 lookup primitive. It means zero or one right-side row per current row, and it must not
silently deduplicate duplicate right rows. C27 does not change that contract.

The analytical join family begins in v2. Existence joins, `join_many(...)`, and deterministic deduped `join_one(...)`
are admitted in the default PySpark profile. Hooks remain the honest escape hatch for join shapes that are not yet
specified, represented in IR, checked by backend capabilities, lowered through shared PySpark recipes, and covered by
online/generated parity tests.

## Feature Ladder

The v2 order should follow production frequency and semantic risk:

1. Existence joins: semi and anti filters that keep or remove current rows based on right-side matches.
2. `join_many(...)`: row multiplication is intentional and visible in traceability.
3. Deterministic lookup dedupe: a `join_one(...)` policy that selects one right row before joining.
4. Temporal and as-of lookups: time-aware joins that select records relative to an event time.
5. SCD-style lookups: validity-window joins with explicit overlap handling.

Right, full, and cross joins remain deferred. They do not fit Structure's current row-centric transform model cleanly
because they can introduce rows without a current-row source or create unbounded multiplication.

## Semantic Principles

Every analytical join form must declare cardinality. The compiler and generated code should make it obvious whether a
join preserves rows, filters rows, multiplies rows, or selects one right row.

Policies that choose one right row must be deterministic. If the source cannot prove a total ordering or a unique
choice, Structure should warn or require an explicit tie policy. It must never lower to arbitrary Spark `first(...)` or
`dropDuplicates(...)` behavior.

Every join form must be optimizer-visible. The preferred path is symbolic DSL, checked IR, shared PySpark recipes, and
traceability. Hooks are acceptable for rare or urgent cases, but diagnostics and docs should steer common patterns back to
compiler-visible syntax.

No compiler command may import PySpark or inspect data to prove join facts. Runtime checks for duplicate matches,
overlapping validity windows, or tie violations must be explicit because they can add Spark work.

## Public DSL Direction

Existence joins should read like predicates because they do not expose right-side fields:

```python
where(self.customers.exists(on=self.customers.id == order.customer_id))
where(self.suppressed_emails.not_exists(on=self.suppressed_emails.email == order.email))
```

`exists(...)` is semi join semantics: keep current rows that have at least one right match. `not_exists(...)` is anti
join semantics: keep current rows that have no right match. Both return symbolic boolean predicates, not joined scopes.

Row-multiplying joins should use a distinct method:

```python
item = self.order_items.join_many(
    on=self.order_items.order_id == order.id,
    how=Join.INNER,
)
```

`join_many(...)` returns a joined symbolic scope like `join_one(...)`, but row multiplication is intended. Output
schema construction still decides which fields survive.

Deterministic dedupe should be explicit on `join_one(...)`:

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

The policy means "reduce the right side to one row per join key, then apply `join_one(...)`." Current PySpark lowering
uses `row_number()` over the right-side join keys and explicit order expression. `TiePolicy.ERROR` is recorded in IR
and traceability; runtime tie checks are still explicit follow-up work because they add Spark work.

Temporal joins should name the event time and the right-side validity facts:

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

The default interval is closed-open: `valid_from <= at < valid_to`. A null `valid_to` represents an open-ended current
record. `OverlapPolicy.ERROR` means overlapping right windows are invalid for the chosen key.

As-of lookups are a specialized temporal lookup where the nearest right-side timestamp is selected:

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

`AsOf.BACKWARD` means choose the latest right row at or before the left time. Other directions should wait until the
first backward implementation is stable.

## IR and Lowering

The IR should extend `JoinOperation` rather than create unrelated operation families. Required fields include:

- method: `exists`, `not_exists`, `join_many`, `join_one` with dedupe, `temporal_one`, or `as_of_one`;
- cardinality: preserves rows, filters rows, multiplies rows, or selects one right row;
- right input scope and deterministic occurrence id;
- ordered key pairs and equality kind;
- optional dedupe, temporal, as-of, tie, and overlap policies;
- expected right-side fields;
- source location and diagnostic expression text;
- runtime check requirements, when a policy asks for data-dependent validation.

The PySpark target plan stores analytical join metadata on shared `PySparkJoinRecipe` records. Existence joins map to
left semi/anti join modes, `join_many(...)` maps to ordinary row-multiplying join modes plus optional strategy hints,
and deduped `join_one(...)` carries a `PySparkJoinDedupeRecipe` with direction, order expression, and tie policy.
Temporal and as-of joins may add dedicated policy records when implemented.

Online execution and generated code must consume those recipes through the shared execution semantic contract.

## Diagnostics and Traceability

Diagnostics should say which cardinality shape the join has and why a source form is unsupported. Join warnings must
distinguish between unproven uniqueness, unproven dedupe ties, and unproven temporal-window overlap.

Traceability should show existence joins as filters, `join_many(...)` as row-multiplying dependencies, dedupe policies as
right-side prejoin reductions, and temporal joins as dependencies on both key fields and time-validity fields.

## Implementation Notes

- Add backend capability requirements before lowering a new join form.
- Add syntax only with semantic tests and online/generated parity tests.
- Keep v2 existence joins, `join_many(...)`, and deterministic lookup dedupe independent of temporal joins.
- Keep temporal and as-of joins batch-only until streaming compatibility is specified.
- Add examples showing when a hook is still the right escape hatch.
