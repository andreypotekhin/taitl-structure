# Design: Full PySpark Join Support

## Purpose

Sprint 07 made the common analytical join family compiler-visible, but it deliberately left out the join forms that can
create rows without a current-row owner or that behave like broad PySpark DataFrame joins. Full PySpark join support
admits those forms without weakening Structure's core rule: users write explicit, typed, compiler-visible transforms
and generated PySpark remains reviewable.

The design goal is not to mirror every stringly PySpark call. It is to cover the behavior users reach for in
`DataFrame.join(...)`: right and full outer joins, cross joins, arbitrary boolean join predicates, PySpark join strategy
hints, and reviewable explain output. Structure should expose those behaviors through typed scopes, explicit
cardinality, backend capabilities, diagnostics, traceability, and shared execution/generated-code PySpark recipes.

## First-Slice Boundary

The first analytical join slice already covers:

- `lookup_join(...)` lookup joins;
- `exists(...)` and `not_exists(...)` semi and anti filters;
- `inner_join(...)` row-multiplying left and inner joins;
- deterministic lookup dedupe;
- temporal validity-window lookups;
- backward as-of lookups.

The remaining PySpark join support is:

- right outer joins;
- full outer joins;
- explicit cross joins;
- arbitrary compileable boolean join predicates, including non-equi and disjunctive predicates;
- optional PySpark-style same-name equi-key shorthand;
- join strategy directives beyond broadcast;
- explain and diagnostics rich enough to show row-admitting joins clearly.

Automatic cost-based join reordering remains outside this design. Spark may optimize physical plans, but Structure
preserves source-order logical operations unless a later optimizer design defines a safe rewrite contract.

## Public DSL Direction

Keep existing APIs unchanged. `lookup_join(...)` stays a select-one lookup. `inner_join(...)` stays the ordinary row
multiplying left or inner join for a current row. Existence predicates stay predicates.

Add one explicit rowset join primitive for the PySpark shapes that no longer have a guaranteed current-row owner:

```python
rowset_join(
    left=order,
    right=customer,
    on=order.customer_id == customer.id,
    how=Join.FULL,
)

return OrderCustomerReconciliation.project()(
    order_id=order.id,
    customer_id=customer.id,
    matched=order.id.is_not_null() & customer.id.is_not_null(),
)
```

`rowset_join(...)` records a rowset join and returns the joined right relation scope, following the existing Structure
join pattern. The shortcut helpers `left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and
`cross_join(...)` call the same primitive. They can be called without assignment when the right relation can be inferred
from `on`.

Right joins should usually be rendered as PySpark right joins for readability, not rewritten to left joins in generated
code. Rewrites may become an optimizer feature later, but the first implementation should preserve source intent.

Cross joins require an explicit acknowledgement:

```python
cross_join(
    calendar_day,
    allow_cartesian=True,
)
```

`allow_cartesian=True` is required so an accidentally missing `on` clause cannot create a Cartesian product. A cross
join must not accept `on`.

`rowset_join(...)` accepts a general symbolic boolean predicate for `on`. The predicate may include equality,
null-safe equality, inequalities, boolean `AND`, boolean `OR`, deterministic expression helpers, and literals. The
predicate may reference only the left and right rowset scopes and earlier joined scopes explicitly passed through the
current step method. It must not call arbitrary Python functions, inspect data, or use string SQL fragments.

Optional same-name key shorthand can be added after predicate joins work:

```python
joined = rowset_join(
    left=order,
    right=shipment,
    using=(order.id, order.tenant_id),
    how=Join.INNER,
)
```

The shorthand means each named left field is matched to the right field with the same schema field name. It is a
source convenience only. Output fields are still explicit; Structure does not rely on PySpark's duplicate-column
coalescing behavior.

## Cardinality and Output Model

`rowset_join(...)` is a rowset operation, not a lookup. It can preserve, filter, multiply, or admit rows depending on
`how`:

- `Join.INNER`: keeps matching left/right pairs;
- `Join.LEFT`: keeps all left rows and matching right rows;
- `Join.RIGHT`: keeps all right rows and matching left rows;
- `Join.FULL`: keeps matching pairs and unmatched rows from both sides;
- `Join.CROSS`: emits every left/right pair.

Because right and full joins can produce rows with no current-row left source, schema construction must use
`project()` or an equivalent no-base constructor. `OutputSchema.base(order)` is invalid when the chosen join can admit
right-only rows. Diagnostics should point to this design and explain how to project nullable fields explicitly.

## IR and Lowering

Extend `JoinOperation` or add a sibling `RowsetJoinOperation` only if the existing model becomes unclear. The required
semantic fields are:

- method: `rowset_join`;
- join type: inner, left, right, full, or cross;
- cardinality: row-pairing, row-preserving-left, row-preserving-right, row-preserving-both, or Cartesian;
- left scope and right scope;
- occurrence id and generated aliases for both sides;
- predicate expression, or same-name key shorthand before expansion;
- predicate class: equi, non-equi, disjunctive, mixed, or Cartesian;
- nullable-side metadata for type checks;
- referenced fields for right/left projection;
- strategy directives;
- source location and source expression text.

The PySpark recipe layer should render:

- `.join(right, predicate, "right")` for right joins;
- `.join(right, predicate, "full")` for full joins;
- `.crossJoin(right)` or `.join(right, how="cross")` for cross joins, chosen by target capability;
- ordinary `.join(...)` for inner and left rowset joins;
- explicit `select(...)` after the join to produce declared output fields and avoid duplicate unqualified names.

Execution must consume the same recipe shape as generated code. The compiler and code generator must stay
Spark-free.

## Backend Capabilities

Add capabilities before lowering:

```text
join.rowset_join
join.right_join
join.full_join
join.cross_join
join.non_equi_condition
join.disjunctive_condition
join.using_keys
join.strategy_broadcast
join.strategy_shuffle_hash
join.strategy_shuffle_replicate_nl
join.strategy_merge
```

`join.using_keys` is optional and should stay unsupported until implemented. The default PySpark profile may admit the
core capabilities as implementation lands. Spark Connect support for these features must be checked through the same
capability mechanism.

## Diagnostics and Explain

Diagnostics must distinguish unsupported semantics from dangerous semantics. A missing `allow_cartesian=True` is a
source error with a clear fix. An unsupported non-equi predicate is a backend capability error. A `base(...)`
constructor after a full join is an output construction error because no single current-row base exists.

Explain output should mark:

- join kind;
- row-admitting side;
- predicate class;
- estimated cardinality shape, without pretending to know data sizes;
- nullable sides;
- strategy directives;
- fields carried from each side.

## Streaming Compatibility

Keep right, full, cross, non-equi, and disjunctive rowset joins batch-only for the first implementation. Stream-static
left and inner joins stay governed by the existing streaming compatibility contract. Stream-stream joins remain
deferred until the streaming orchestration design defines state, watermark, output mode, and checkpoint behavior.

## Open Design Choices

The first implementation favors `rowset_join(...)` as one broad primitive plus thin convenience aliases:
`left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and `cross_join(...)`.

Same-name key shorthand should wait until the explicit predicate path is stable. It is useful for PySpark familiarity,
but Structure's typed field references already avoid the ambiguity that PySpark string keys solve.

Join reordering should not be folded into full join support. It requires a separate proof that filters, hooks,
validations, traceability, and output nullability remain equivalent.
