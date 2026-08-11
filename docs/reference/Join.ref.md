# Join Reference

Structure joins are typed, compiler-visible operations. Choose the family from the desired row behavior before
writing the predicate: preserve current rows, filter by membership, multiply rows, select one right row, or admit
right-side rows. The [Join background](../background/Join.back.md) explains cardinality, nulls, and temporal semantics;
the [Joins API](../api/Joins.api.md) is the callable inventory.

Examples use the order and customer schemas introduced in the [Schema reference](Schema.ref.md). Replace those names
with schemas from your application.

Most transformations start with the common rowset helpers: use `left_join(...)` for optional enrichment and
`inner_join(...)` when an unmatched current row should be removed or matching rows should be expanded. Reach for
`lookup_join(...)` when the business rule specifically selects at most one right-side row.

## Join families

| Desired behavior | Operation |
| --- | --- |
| Enrich every current row, with nullable right fields | `left_join(...)` |
| Keep only current rows with a match or one row per match | `inner_join(...)` |
| Remove current rows with a match | `not_exists(...)` |
| Preserve every matching pair | `rowset_join(...)` or another explicit `how` |
| Select at most one right row | `lookup_join(...)` |
| Select one valid-time or nearest-time row | `temporal_one(...)` or `as_of_one(...)` |
| Admit right-only rows | `right_join(...)` or `full_join(...)` |
| Pair every row deliberately | `cross_join(..., allow_cartesian=True)` |
| Join a parameter-style relation | `param_join(relation)` |

## Common rowset joins

Use `left_join(...)` for the ordinary optional-enrichment case. Use `inner_join(...)` when missing matches should be
removed or when each matching pair belongs in the result.

```python
from structure import *
from structure.plugin.pyspark import *


def add_customer(self, order: Order, customer: Customer) -> EnrichedOrder:
    left_join(
        on=(order.tenant_id == customer.tenant_id) & (order.customer_id == customer.id),
        hint="broadcast",
    )
    return EnrichedOrder.base(order)(customer_name=customer.name)
```

`left_join(...)` preserves the current row and makes joined fields nullable when no customer matches. If a matching
right row is required, replace it with `inner_join(...)`.

## Lookup joins

Use a lookup join when the current row should receive at most one matching right-side row.

`lookup_join(...)` is the short form for select-one enrichment and defaults to a left join. It is row-preserving only
when the right side is unique for the condition. If uniqueness is not proven, Structure reports a warning or requires
an explicit dedupe policy.

| Option | Meaning |
| --- | --- |
| `on=` | Symbolic Boolean predicate or same-name key shorthand |
| `how="left"` / `"inner"` | Preserve or remove unmatched current rows |
| `hint="broadcast"` | Backend join hint |
| `strategy="shuffle_hash"` | Explicit admitted strategy |
| `dedupe=JoinDedupe.latest_by(...)` | Select one right row before enrichment |

Same-name shorthand is also available when matching schemas with identical physical keys:

```python
left_join(on="tenant_id")
inner_join(on=["tenant_id", "order_id"])
```

Use a full symbolic predicate when keys are renamed, tenant scope must be combined, or null-safe equality is required.

### Lookup uniqueness

`lookup_join(...)` is intended to enrich the current relation with at most one right row. The compiler cannot infer
uniqueness from a fixture or from a human naming convention. If the source may contain several matches, make the
selection explicit:

```python
lookup_join(
    on=(order.product_id == version.product_id) & (order.tenant_id == version.tenant_id),
    dedupe=JoinDedupe.latest_by(version.valid_from),
)
```

| Right-side condition | Recommended operation |
| --- | --- |
| Proven unique key | `lookup_join(...)` |
| Several versions, choose one | `lookup_join(..., dedupe=JoinDedupe.latest_by(...))` |
| Several matches are meaningful | `inner_join(...)` or `rowset_join(...)` |
| Need membership only | `exists(...)` |
| Need one valid-time row | `temporal_one(...)` |
| Need one nearest-time row | `as_of_one(...)` |

Do not use a lookup join to hide a one-to-many relationship. A duplicate right match multiplies current rows and changes
the downstream grain.

## Rowset joins

Use a rowset join when matching pairs, rather than select-one enrichment, are part of the result.

```python
rowset_join(
    customer,
    on=(order.customer_id == customer.id) & (order.tenant_id == customer.tenant_id),
    how="left",
)
```

| Operation | Meaning |
| --- | --- |
| `rowset_join(right, on=..., how=...)` | General typed rowset join |
| `left_join(on=...)` | Left row-preserving shortcut |
| `inner_join(on=...)` | Matching pairs only |
| `right_join(on=...)` | Right-preserving join |
| `full_join(on=...)` | Preserve both sides |
| `cross_join(right, allow_cartesian=True)` | Explicit Cartesian product |
| `param_join(right)` | Parameter-style Cartesian join with a batch-only singleton assertion |
| `relation_alias(relation, name=...)` | Named self-join or repeated relation occurrence |

Right and full joins can make left fields nullable; build the result with an explicit output Schema. A cross join must
include `allow_cartesian=True` and does not accept `on=`. `param_join(right)` performs the singleton assertion only
when the actual step is batch; use `cross_join(right, allow_cartesian=True)` when multiple rows are intentional. The
alias must be a unique non-empty Python identifier within the step and does not execute or duplicate the relation.

```python
class OrderWithCustomer(Schema):
    order_id = string(nullable=True)
    customer_name = string(nullable=True)


def enrich(order: Order, customer: Customer) -> OrderWithCustomer:
    right_join(customer, on=order.customer_id == customer.id)
    return OrderWithCustomer(
        order_id=order.id,
        customer_name=customer.name,
    )
```

The nullable field reflects that a right join can produce a row with no matching left-side order.

Predicates may use equality, null-safe equality, inequalities, ranges, symbolic Boolean `&`/`|`, and literals that
keep the result Boolean. Raw SQL strings are not join predicates.

### Null semantics

An ordinary equality predicate does not match two null keys. Use `null_safe_eq(...)` only when null is a meaningful
business key value. A left join makes right fields nullable for unmatched current rows. An inner join removes unmatched
rows and retains the right field's declared nullability unless a later filter narrows it. Right and full joins can also
make left fields nullable.

Keep tenant and other scope keys in the predicate. A relation with a globally unique-looking product ID is not proof
that a multi-tenant join is safe:

```python
left_join(
    on=(order.tenant_id == product.tenant_id)
    & (order.product_id == product.id)
)
```

If a missing lookup should reject the current row, use `inner_join` or `exists`; if it should remain visible with null
enrichment, use `left_join` and project the nullable fields explicitly.

## Existence joins

Use an existence join when the right relation decides eligibility but its fields do not belong in the result.

```python
where(exists(on=(order.customer_id == customer.id) & (order.tenant_id == customer.tenant_id)))
where(not_exists(on=order.product_id == blocked.product_id))
```

`exists(...)` and `not_exists(...)` filter current rows without exposing right-side fields. They are useful for
eligibility and exclusion predicates. If right-side attributes are needed, use a rowset or lookup join and project
them explicitly.

## Temporal and as-of joins

Use a temporal or as-of join when the matching right row depends on validity or event time.

```python
temporal_one(
    on=(order.product_id == price.product_id) & (order.tenant_id == price.tenant_id),
    at=order.business_date,
    valid_from=price.valid_from,
    valid_to=price.valid_to,
    overlaps="error",
    how="left",
)

as_of_one(
    on=order.product_id == rate.product_id,
    left_time=order.created_at,
    right_time=rate.observed_at,
    direction="backward",
    tolerance="7 days",
    ties="error",
)
```

`temporal_one(...)` uses a closed-open validity interval: a row is valid at `at` when it is on or after
`valid_from` and before `valid_to`. `overlaps="error"` keeps overlapping right candidates from becoming an
arbitrary choice.

`as_of_one(...)` supports:

- `direction="backward"`: latest right time at or before the left time;
- `direction="forward"`: earliest right time at or after the left time;
- `direction="nearest"`: closest non-null right time;
- `tolerance=...`: reject candidates beyond the permitted duration;
- `ties="error"`: fail when the best candidate is ambiguous.

Nearest ties fail by default. Directional nearest tie preferences are not part of the public contract; make the right
time unique or narrow candidates with `tolerance=`.

Temporal and as-of operations are select-one operations. They require a key predicate in addition to the time rule;
the time field alone does not connect unrelated rows. A closed-open interval prevents adjacent
validity boundaries. Null time candidates are ignored by as-of selection. `how="left"` retains an unmatched current row;
`how="inner"` removes it.

Use `overlaps="error"` and `ties="error"` while establishing a source contract. An explicit error is safer than a
different price, rate, or promotion being selected after a source replay.

## Projection and scope

Joined scopes keep aliases and field identity visible to the compiler:

```python
order_scope = relation_alias(order, name="order")
customer_scope = relation_alias(customer, name="customer")
left_join(on=order_scope.customer_id == customer_scope.id)

return EnrichedOrder.project(order_scope)(
    id=order_scope.id,
    customer_name=customer_scope.name,
)
```

Prefer a declared projection after a join. It avoids duplicate names, makes nullable sides explicit, and gives online
and generated execution the same output shape. A join does not automatically publish every right-side field.

### Projection patterns

Project into a result Schema when joined fields need deliberate names and nullability.

```python
class EnrichedOrder(Schema):
    order_id = string(nullable=False)
    customer_name = string(nullable=True)
    customer_tier = string(nullable=True)


def enrich(order: Order, customer: Customer) -> EnrichedOrder:
    left_join(on=order.customer_id == customer.id)
    return EnrichedOrder(
        order_id=order.id,
        customer_name=customer.name,
        customer_tier=customer.tier,
    )
```

For a right or full join, declare the left-derived fields nullable in the result Schema. For repeated joins to the same
relation, use `relation_alias(...)` for each occurrence and project from the named scopes. Do not depend on backend
duplicate-column resolution or physical field order.

## Hints and strategies

Hints and strategies are optimization directives, not replacements for join meaning. Structure validates their spelling
and preserves them in explain output. Use `broadcast` only when the right side is suitable for that strategy. A
strategy does not prove lookup uniqueness, remove duplicate rows, or make a stream join bounded.

`hint=` and `strategy=` are target-sensitive. A supported hint may change physical planning while leaving the logical
join contract unchanged. An unsupported strategy fails capability checking rather than silently degrading to a different
join family. Explain output should retain the chosen hint, join type, aliases, and dedupe policy for review.

```python
lookup_join(
    on=(order.tenant_id == customer.tenant_id) & (order.customer_id == customer.id),
    dedupe=JoinDedupe.latest_by(customer.updated_at),
    hint="broadcast",
)
return EnrichedOrder.base(order)(customer_name=customer.name)
```

The hint may improve physical planning, while `JoinDedupe.latest_by(...)` carries the logical select-one contract.

## Join ordering and composition

Joins execute in source order with filters and projections. A later predicate may use a scope introduced by an earlier
join; it may not reference a right relation that has not been joined. Keep a join followed by its output projection
together when the intermediate duplicate or nullable shape should not leak into later steps.

```python
def publish(order: Order, customer: Customer, product: Product) -> PublishedOrder:
    left_join(customer, on=(order.tenant_id == customer.tenant_id) & (order.customer_id == customer.id))
    inner_join(product, on=(order.tenant_id == product.tenant_id) & (order.product_id == product.id))
    return PublishedOrder(
        id=order.id,
        customer_name=customer.name,
        product_name=product.name,
    )
```

The `inner_join` in this example admits only rows with an observed product while the left customer enrichment remains
nullable. Reordering those joins can change which rows are available to later filters and must be an explicit source
change.

## Streaming boundary

Stream-static left/inner joins, `exists(...)` filtering, and admitted bounded stream-stream joins are available under
the [Streaming API](../api/Streaming.api.md) conditions. Callers own streaming sources, sinks, triggers, checkpoints,
output modes, and query lifecycle. Temporal or as-of joins are not automatically streaming-safe merely because their
predicate includes a timestamp; the configured target and bounded-state contract must admit them.

Watermarks must precede the stateful stream operation they support. A timestamp comparison without a watermark and a
bounded range is not a streaming state contract. Broad rowset joins, unbounded cross joins, and arbitrary stateful
lookup behavior remain rejected when compatibility analysis cannot establish bounded state and restart semantics.

```python
@transform(streaming=True)
class EnrichEvents(Transform):
    events = input(Event, streaming=True)
    allowed = input(AllowedKey, streaming=True)
    output = output(EnrichedEvent)

    @step(input=[events, allowed], output=output, streaming=True)
    def enrich(self, event: Event, key: AllowedKey) -> EnrichedEvent:
        watermark(event.event_time, delay="1 hour")
        inner_join(key, on=event.key == key.key)
        return EnrichedEvent.base(event)(label=key.label)
```

This is a compatibility declaration only. The caller controls both streaming inputs and the query that consumes
`output`.

## Diagnostics and corrections

Join diagnostics should identify the current relation, right relation, join family, predicate, cardinality policy, and
shortest correction. Before accepting a join, verify:

- the predicate includes every business-scope key;
- null-safe equality is used only intentionally;
- the expected output grain is row-preserving, filtering, multiplying, or select-one;
- lookup uniqueness, dedupe, overlap, and tie policies are explicit;
- right and full join nullability is reflected in the output Schema;
- aliases are unique within the step;
- a cross join is bounded and explicitly acknowledged;
- strategy/hint options are supported by the selected target;
- streaming joins have watermarks, bounds, and application-controlled lifecycle.

Example correction:

```text
Join warning: lookup_join may return more than one right row

Use:
  Prove the right key unique, add JoinDedupe.latest_by(...), or use inner_join(...) when row multiplication is intended.
```

## Common corrections

| Situation | Correction |
| --- | --- |
| A lookup produces duplicate current rows | Prove uniqueness or add `JoinDedupe.latest_by(...)` / `earliest_by(...)` |
| A tenant-scoped relation matches across tenants | Include tenant identity in the predicate |
| A full/right join fails schema validation | Project into a Schema that marks nullable sides correctly |
| A Cartesian join is rejected | Add `allow_cartesian=True` and confirm the bounded use case |
| A temporal lookup selects an ambiguous row | Set `overlaps="error"` or make validity intervals unique |
| A raw SQL predicate is rejected | Compose typed field expressions and Boolean operators |

## Operation inventory

| Family | Operations |
| --- | --- |
| Select-one enrichment | `lookup_join`, `JoinDedupe.latest_by`, `JoinDedupe.earliest_by` |
| Membership | `exists`, `not_exists` |
| Rowset | `rowset_join`, `left_join`, `inner_join`, `right_join`, `full_join`, `cross_join` |
| Temporal | `temporal_one` |
| As-of | `as_of_one` with `backward`, `forward`, or `nearest` |
| Scope | `relation_alias` |

The short forms preserve the same typed plan as `rowset_join`. They do not expose raw PySpark `DataFrame.join` or raw
SQL predicates as a second untyped API. Use `how="left"` or `how="inner"` deliberately for lookup and temporal
operations; the default is chosen for enrichment, not as a general cardinality guarantee.

## Conditions and literals

Join conditions may combine typed equality, `null_safe_eq`, inequalities, ranges, symbolic Boolean operators, and
compiler-visible scalar literals. The predicate must remain Boolean. Python `and`, `or`, and expression truthiness are
not valid alternatives to `&`, `|`, and `~`.

```python
on = (
    (left.tenant_id == right.tenant_id)
    & left.code.null_safe_eq(right.code)
    & (right.valid_from <= left.observed_at)
)
left_join(on=on)
```

Do not build a predicate from raw SQL text or an untyped Python callback. The compiler needs the source fields,
nullability, aliases, and target capability to remain visible in explain output and generated code.

## Cardinality guide

| Operation | Current row retained? | Right fields exposed? | May multiply rows? |
| --- | --- | --- | --- |
| `exists` | Only when match exists | No | No |
| `not_exists` | Only when no match exists | No | No |
| Left lookup | Yes | Yes, nullable when unmatched | Only if uniqueness is not established |
| Inner rowset | No unmatched rows | Yes | Yes |
| Right/full rowset | Right/full rows retained | Yes | Yes |
| Temporal/as-of one | Depends on `how` | Yes | Not when tie/overlap policy succeeds |
| Cross join | Every current row | Yes | Deliberately, by product |

Use this table to choose the operation before writing the projection. If a transformation needs both membership and
right-side fields, use two explicit operations or a rowset join rather than relying on a hidden semi-join projection.

```python
where(exists(on=order.customer_id == customer.id))
left_join(customer, on=order.customer_id == customer.id)
return EnrichedOrder.base(order)(customer_name=customer.name)
```

The existence predicate controls eligibility; the later left join supplies optional display data.

## Temporal edge cases

For `temporal_one`, the normal interval is `[valid_from, valid_to)`. Adjacent intervals can meet at one boundary
without both being valid at the same instant. Null validity endpoints, overlapping intervals, and missing right rows
require the selected target's documented contract and should not be left to backend defaults.

For `as_of_one`, null right timestamps are not candidates. A tolerance applies after the direction is chosen. A nearest
match at equal distance is ambiguous unless the source is made unique or an admitted tie policy is supplied. A left
join keeps an unmatched current row with nullable right fields; an inner join removes it.

```python
as_of_one(
    on=order.product_id == price.product_id,
    left_time=order.created_at,
    right_time=price.observed_at,
    direction="backward",
    tolerance="7 days",
    ties="error",
    how="left",
)
return OrderPrice.base(order)(unit_price=price.amount)
```

The explicit tie and tolerance policy prevents a replay or ambiguous price snapshot from silently changing the result.

## Output shape

Before publishing a joined relation, verify:

1. The output Schema has no accidental duplicate Python or physical names.
2. Aliases identify repeated occurrences and do not collide within the step.
3. Every nullable join side is declared nullable in the result.
4. Right-side fields are projected intentionally rather than carried wholesale.
5. Row grain after the join is documented for downstream aggregation.
6. Dedupe, overlap, tie, hint, and strategy policies are retained in explain output.
7. Source-order filters do not depend on a scope introduced later.

The compiler can preserve a legal join plan, but it cannot infer the business meaning of a duplicated row, a missing
tenant key, or an unknown shipment match. Those meanings belong in the source predicate and output Schema.

## Stable join decisions

For a production-facing join, record the left and right snapshot identities, expected cardinality, key completeness,
null policy, duplicate/tie policy, output projection, and target profile. This makes a later change in lookup source or
interval policy reviewable instead of looking like an unexplained row-count change.

When a join feeds an aggregate, verify the row grain immediately before grouping. A one-to-many join followed by
`sum(...)` can double-count facts; a lookup dedupe followed by a projection can preserve one row per current key. The
compiler can report cardinality warnings, but the business owner must choose whether multiplication is intended.

## Join notes

For each persisted or user-visible join, record:

```text
left relation and snapshot
right relation and snapshot
join family and how
complete key predicate
expected cardinality
null-safe equality policy
dedupe/overlap/tie policy
output Schema and aliases
target and streaming profile
```

This record is especially useful for temporal prices, promotions, rates, customer attributes, shipment facts, and
multi-tenant catalog lookups, where a change in the right-side snapshot can change output even when the left input is
unchanged.

## See also

- [Transform reference](Transform.ref.md)
- [Aggregations reference](Aggregations.ref.md)
- [Joins API](../api/Joins.api.md)
- [Join background](../background/Join.back.md)
