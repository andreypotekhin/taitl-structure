# Full PySpark Join Support

## Purpose

Full PySpark join support covers the join forms that the first analytical join slice deferred: right outer joins, full
outer joins, explicit cross joins, arbitrary compileable boolean join predicates, PySpark join strategy directives, and
clear explain output for row-admitting joins.

This specification builds on [JoinSemantics.md](JoinSemantics.md) and
[AnalyticalJoinCoverage.md](AnalyticalJoinCoverage.md). It does not change `lookup_join(...)`, `exists(...)`,
`not_exists(...)`, `inner_join(...)`, deterministic lookup dedupe, temporal lookups, or as-of lookups.

## Scope

In scope:

- `rowset_join(...)` for inner, left, right, full, and cross rowset joins;
- non-equi predicates such as range overlap predicates;
- boolean `OR` and mixed `AND`/`OR` predicates when all expression nodes are compileable;
- explicit Cartesian-product acknowledgement;
- nullable-side type checking after right and full joins;
- output construction rules for row-admitting joins;
- backend capability checks;
- PySpark generated and execution lowering through shared recipes;
- traceability and rich explain output;
- batch-only streaming classification for the new join forms.

Out of scope:

- automatic cost-based join reordering;
- automatic physical-plan guarantees;
- stream-stream joins;
- lateral joins and table-valued-function joins;
- storage write planning after broad row multiplication;
- string SQL join conditions;
- arbitrary Python join predicates.

## Terms

A rowset join is a join that produces joined row pairs rather than enriching one current row with one right-side scope.
It can produce rows where the left side is missing, the right side is missing, or both sides are present.

A row-admitting join can create output rows that were not owned by the current left row stream. Right and full joins are
row-admitting from Structure's row-centric perspective.

A Cartesian join is a cross join. Every left row is paired with every right row.

A compileable predicate is a symbolic boolean expression Structure can lower to PySpark without arbitrary Python or SQL
strings.

## Public API

Canonical API:

```python
rowset_join(relation=None, *, left=None, right=None, how, on=None, strategy=None, allow_cartesian=False)
```

Rules:

- The right relation can be supplied positionally, with `right=...`, or inferred from `on` when the predicate names
  exactly one unjoined relation.
- `left` is optional documentation of the current row scope for explicit rowset joins.
- `how` is required.
- Supported initial values are `"inner"`, `"left"`, `"right"`, `"full"`, and `"cross"`.
- `on` is required for every value except `"cross"`.
- `on` is forbidden for `"cross"`.
- `allow_cartesian=True` is required for `"cross"`.
- `strategy` is optional and follows Sprint 09 join strategy directive rules.
- The return value is the joined right relation scope. The established no-assignment style is allowed when relation
  inference is unambiguous.

Shortcut helpers:

```python
left_join(on=customer.id == order.customer_id)
inner_join(on=customer.id == order.customer_id)
right_join(on=customer.id == order.customer_id)
full_join(on=customer.id == order.customer_id)
cross_join(calendar_day, allow_cartesian=True)
```

Example:

```python
def reconcile(self, order: OrderRaw, customer: Customer) -> OrderCustomerReconciliation:
    full_join(on=order.customer_id == customer.id)
    return OrderCustomerReconciliation.project()(
        order_id=order.id,
        customer_id=customer.id,
        matched=order.id.is_not_null() & customer.id.is_not_null(),
    )
```

## Join Types

`"inner"` keeps matching row pairs.

`"left"` keeps every left row and matching right rows. Unmatched right fields are null.

`"right"` keeps every right row and matching left rows. Unmatched left fields are null.

`"full"` keeps matching row pairs and unmatched rows from both sides. Left fields are null for right-only rows.
Right fields are null for left-only rows.

`"cross"` emits every left/right pair. It has no predicate and requires `allow_cartesian=True`.

Semi and anti joins remain `exists(...)` and `not_exists(...)`, not `rowset_join(...)`.

## Predicates

`rowset_join(..., on=...)` accepts compileable boolean expressions:

- equality;
- null-safe equality;
- inequalities;
- deterministic expression helpers;
- boolean `AND`;
- boolean `OR`;
- literals where the expression remains boolean;
- parenthesized combinations of the above.

Rejected predicates:

- string SQL fragments;
- raw column-name strings;
- arbitrary Python functions;
- Python lambdas;
- Python collection membership unless represented by a compileable expression helper;
- predicates that reference neither side of the join;
- predicates that reference an unjoined scope not supplied as `left`, `right`, or an already available joined scope.

The compiler classifies predicates as `equi`, `non_equi`, `disjunctive`, or `mixed` for capability checks, diagnostics,
and explain output.

## Same-Name Key Shorthand

`using` may be admitted later after explicit predicate joins are stable:

```python
joined = rowset_join(
    left=order,
    right=shipment,
    using=(order.id, order.tenant_id),
    how="inner",
)
```

Rules:

- Every field in `using` must exist on both schemas by field name.
- The compiler expands the shorthand into equality key pairs.
- Output construction remains explicit. Structure does not implicitly coalesce or drop duplicate key fields.
- `using` is rejected when field names do not match on both sides.

## Output Construction

After `"right"` or `"full"`, the output cannot use a base constructor that assumes every output row has a current
left row:

```python
return Output.base(order)(...)
```

The correct shape is explicit projection:

```python
return Output.project()(...)
```

Rules:

- Fields from nullable sides are nullable in type checking after the join.
- Output schema construction chooses every final field explicitly.
- Structure never appends all right-side or left-side fields automatically.
- Duplicate source field names are harmless while they remain scoped.

## IR Contract

The IR records:

- operation method `rowset_join`;
- join type;
- left scope and right scope;
- generated aliases for both sides;
- occurrence id;
- predicate expression, or expanded `using` key pairs;
- predicate class;
- cardinality class;
- nullable sides;
- referenced fields per side;
- strategy directive;
- `allow_cartesian` flag;
- source location and expression text.

Cardinality classes:

- `pairs_only` for inner joins;
- `preserve_left` for left joins;
- `preserve_right` for right joins;
- `preserve_both` for full joins;
- `cartesian` for cross joins.

## Backend Capabilities

Required capabilities:

```text
join.rowset_join
join.right_join
join.full_join
join.cross_join
join.non_equi_condition
join.disjunctive_condition
join.using_keys
```

`join.using_keys` remains unsupported until same-name key shorthand is implemented. Join strategy directives use the
optimization capability names selected by Sprint 09. If the implementation keeps
strategy requirements in the join group, use:

```text
join.strategy_broadcast
join.strategy_shuffle_hash
join.strategy_shuffle_replicate_nl
join.strategy_merge
```

Unsupported capability diagnostics use `BACKEND-E2402` and link to this specification or the public reference page.

## PySpark Lowering

Generated-code execution and execution consume the same PySpark join recipe.

Lowering rules:

- `"right"` renders a right join.
- `"full"` renders a full outer join.
- `"cross"` renders `crossJoin(...)` or an equivalent target-supported cross join.
- Non-equi and disjunctive predicates render as PySpark Column expressions.
- Strategy directives render as explicit right-side or join hints according to the optimization directive
  specification.
- The recipe always follows the join with explicit projection into the declared output schema.

The compiler must not import PySpark, start Spark, inspect live data, or evaluate PySpark `Column` values while
checking or generating these joins.

## Diagnostics

Diagnostics include:

- transform and step method;
- join type and operation method;
- left and right scopes;
- source predicate;
- predicate class;
- nullable sides;
- cardinality class;
- strategy directive when present;
- suggested source fix;
- link to this specification or the public reference page.

Example:

```text
CompileError JOIN-E2801: cross join requires explicit Cartesian acknowledgement

Join:
  CalendarExpansion.expand -> orders x calendar_days

Problem:
  "cross" can multiply every left row by every right row. Structure requires allow_cartesian=True so accidental
  missing predicates fail early.

Use:
  cross_join(calendar_day, allow_cartesian=True)

See docs/dev/specifications/FullPySparkJoinSupport.md
```

Example:

```text
CompileError JOIN-E2802: full join output cannot use a current-row base

Join:
  ReconcileOrders.reconcile -> orders full customers

Problem:
  "full" may produce rows with no orders row, so Output.base(order) is not valid.

Use:
  Output.project()(...) and reference joined.left and joined.right fields explicitly.

See docs/dev/specifications/FullPySparkJoinSupport.md
```

## Explain and Traceability

Traceability records both sides of a rowset join. Explain output shows:

- join type;
- predicate class;
- nullable sides;
- cardinality class;
- field reads from each side;
- output fields derived from each side;
- strategy directives.

Explain must not imply data-size estimates unless a later cost model adds measured evidence.

## Streaming Compatibility

`rowset_join(...)` is batch-only for `"right"`, `"full"`, `"cross"`, non-equi predicates, and disjunctive
predicates in the first implementation.

Existing stream-static compatibility for left and inner lookup-style joins remains governed by
[StreamingCompatibility.md](StreamingCompatibility.md). Stream-stream joins remain out of scope.

## Acceptance Scenarios

Implementation is complete when tests prove:

- right joins keep right-only rows with nullable left fields;
- full joins keep left-only, right-only, and matched rows;
- cross joins fail without `allow_cartesian=True`;
- cross joins multiply rows when explicitly acknowledged;
- non-equi range predicates lower to generated and online PySpark;
- disjunctive predicates lower to generated and online PySpark;
- nullable-side type checking rejects unsafe assignment to non-null output fields;
- `Output.base(left)` after a full or right join fails with a targeted diagnostic;
- explicit projection after a full join succeeds;
- backend capability rejection occurs before rendering or runtime;
- explain output reports predicate class, cardinality, and nullable sides;
- Spark Connect support either passes or fails through explicit capability diagnostics.
