# Join

Structure joins relate typed row flows while keeping conditions, cardinality, null behavior, aliases, projection, and
backend capabilities visible to the compiler and Spark optimizer. The examples primarily use rowset joins:
`left_join(...)` for optional enrichment, `inner_join(...)` for required matches, and `cross_join(...)` for explicit
candidate or evaluation expansion. The Store rowset fixtures also exercise `right_join(...)` and `full_join(...)`.

`lookup_join(...)` is a narrower select-one primitive. The Store order and demand flows use it when duplicate product
rows must be reduced with an explicit `JoinDedupe` policy. It is not the general join shape used by the examples.

The [Joins API](../api/Joins.api.md) is the concise operation inventory. The [Transform background](Transform.back.md)
explains how joins fit into source-ordered step methods. The design and specification sources are
[JoinSemantics](../dev/specifications/JoinSemantics.md), [Analytical Join
Coverage](../dev/specifications/AnalyticalJoinCoverage.md),
and [Full PySpark Join Support](../dev/specifications/FullPySparkJoinSupport.md).
The corresponding designs are [Analytical Join Coverage](../dev/design/AnalyticalJoinCoverage.md) and
[Full PySpark Join Support](../dev/design/FullPySparkJoinSupport.md).

## A Reader's Join Model

A current row is the row flowing through a source-ordered transform chain. A right input is a named input referenced
through an input scope, such as a typed step parameter or a class input scope. Join families differ in what they
promise:

- A row-preserving join keeps every current row. A left rowset join and a left `lookup_join(...)` are row-preserving.
- A row-filtering join keeps or removes current rows based on whether a match exists. `exists(...)` and
  `not_exists(...)`
  are semi and anti forms.
- A row-multiplying join can produce more than one output row for one current row. `inner_join(...)` is row-multiplying.
- A select-one join chooses at most one right row by uniqueness, explicit dedupe, temporal validity, or as-of ordering.
- A row-admitting join can create rows without a current left row. `right_join(...)` and `full_join(...)` therefore
  require output construction that handles nullable sides explicitly.
- A Cartesian join pairs every left row with every right row. `cross_join(...)` requires explicit acknowledgement.

Choose the narrowest operation that states the business intent, but use rowset joins when broad row cardinality is the
intent. Structure never infers primary keys, uniqueness, business keys, or arbitrary row selection from schema fields.

## Join Families Used by the Examples

The recurring example patterns are:

- `left_join(...)` in catalog, order, fulfillment, recommender, search, and Security flows for optional facts such as
  customers, promotions, shipments, activity, and recommendation feedback.
- `inner_join(...)` in search, Store, Security, and school flows when a missing match should remove the current row or
  when one output row per match is intended.
- `cross_join(...)` in evaluation, scoring, deadlines, reports, remediation, similarity, and candidate flows to pair a
  relation with a batch, policy, evaluation date, or candidate universe.
- `right_join(...)` and `full_join(...)` in the Store `rowset_joins` fixtures for customer backfill and reconciliation.
- `exists(...)` and `not_exists(...)` in catalog and order flows to test membership without exposing right-side fields.
- `temporal_one(...)` in order and demand flows to select a promotion valid at the order date.
- `lookup_join(...)` with `JoinDedupe.latest_by(...)` in order and demand flows to select one product version.

This ordering matters to readers: rowset joins describe most business flows; select-one joins describe the exceptional
case where the right-side cardinality is part of the contract.

## Rowset Join API

`rowset_join(...)` is the general PySpark-style operation. Its canonical shape is:

```python
rowset_join(
    relation=None,
    *,
    left=None,
    right=None,
    on=None,
    how="inner",
    hint=None,
    strategy=None,
    allow_cartesian=False,
)
```

The right relation may be supplied positionally, with `right=...`, or inferred from `on` when exactly one unjoined
relation is referenced. `left=` documents the current row scope when joining from a previously joined scope. `how=` is
one of `"inner"`, `"left"`, `"right"`, `"full"`, or `"cross"`. Every type except `"cross"` requires `on=`; cross joins
forbid `on=` and require `allow_cartesian=True`.

The convenience helpers are shortcuts over `rowset_join(...)`:

```python
left_join(on=customer.id == order.customer_id)
inner_join(on=order.id == line.order_id)
right_join(on=customer.id == reconciliation.customer_id)
full_join(on=order.customer_id == customer.id)
cross_join(calendar_day, allow_cartesian=True)
```

The helpers may receive an explicit relation as their first positional argument. The no-assignment style is valid when
relation inference is unambiguous. The returned relation scope can be assigned when a later expression needs to refer to
the joined occurrence explicitly.

### Worked Optional Enrichment

An ordinary enrichment step makes the current row explicit, joins the typed side input, and constructs only the target
fields:

```python
class EnrichOrders(Transform):
    orders = input(OrderNormalized)
    customers = input(Customer)
    enriched = output(OrderWithCustomer)

    @step(input=[orders, customers], output=enriched)
    def add_customer(
        self, order: OrderNormalized, customer: Customer
    ) -> OrderWithCustomer:
        left_join(
            customer,
            on=(customer.tenant_id == order.tenant_id)
            & (customer.id == order.customer_id),
            hint="broadcast",
        )
        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_segment=customer.segment,
        )
```

The left join preserves the order even when customer details are absent. The output schema must declare those joined
fields nullable, or the step must repair or narrow them explicitly. The broadcast hint can change execution strategy,
but never changes row semantics.

### Worked Row-Multiplying Join

When the output is one row per matching line, use an inner rowset join rather than a select-one lookup:

```python
def expand_lines(self, order: Order, line: OrderLine) -> OrderLineFact:
    inner_join(on=line.order_id == order.id)
    return OrderLineFact(
        order_id=order.id,
        line_number=line.line_number,
        product_id=line.product_id,
        quantity=line.quantity,
    )
```

Duplicate right rows are intentional in this family. No lookup uniqueness warning or hidden dedupe is applied. If the
business rule is instead one selected line per order, use `lookup_join(...)` with an explicit dedupe or temporal policy.

### Rowset Join Types

`"inner"` keeps matching row pairs. Duplicate right rows are allowed, so one current row may become many rows.

`"left"` keeps every left row and matching right rows. Unmatched right fields are nullable.

`"right"` keeps every right row and matching left rows. Unmatched left fields are nullable.

`"full"` keeps matching pairs and unmatched rows from both sides. Left fields are null for right-only rows, and right
fields are null for left-only rows.

`"cross"` emits every left/right pair, has no predicate, and must be explicitly acknowledged. It is appropriate for
small control relations, evaluation calendars, policy combinations, and candidate expansion when that multiplication is
the business operation.

Semi and anti joins remain `exists(...)` and `not_exists(...)`; they are not `rowset_join(...)` types because they do
not
expose right-side fields.

### Same-Name Key Shorthand

The current rowset API accepts same-name key shorthand through `on=`:

```python
left_join(on="customer_id")
inner_join(on=["tenant_id", "order_id"])
```

Structure expands the names into equality key pairs after confirming that the fields exist on both sides. Output
construction remains explicit; Structure does not silently coalesce or remove duplicate key fields. A string SQL
fragment
such as `"customers.id = orders.customer_id"` is not shorthand and is rejected.

### Rowset Predicates

`rowset_join(..., on=...)` accepts compileable symbolic boolean expressions. The full rowset predicate family includes:

- equality and null-safe equality;
- inequalities and range conditions;
- deterministic expression helpers;
- boolean `AND` and `OR`;
- literals where the result remains boolean;
- parenthesized combinations of those expressions.

The examples use composite tenant and identifier predicates, date and status conditions, and the disjunctive promotion
condition in `PrepareCatalog`. Conditions remain visible in generated PySpark and explain output.

Rejected rowset predicates include SQL strings, raw column-name fragments other than the supported same-name shorthand,
arbitrary Python functions, Python lambdas, unsupported collection membership, predicates that reference neither side,
and predicates that reference an unavailable scope. The compiler classifies admitted predicates as `equi`, `non_equi`,
`disjunctive`, or `mixed` for capability checks, diagnostics, and explain output.

## Select-One Lookup Joins

### `lookup_join(...)` Contract

Use `lookup_join(...)` when each current row should match zero or one selected right row. It covers many-to-one and
one-to-one enrichment: many current rows may use the same right row, but a duplicate right key must not arbitrarily
multiply a current row.

```python
lookup_join(
    on=(product.tenant.tenant_id == order.tenant.tenant_id)
    & (product.id == order.product_id),
    how="left",
    dedupe=JoinDedupe.latest_by(product.audit.ingested_at, ties="error"),
)
```

The free-standing call infers the relation when the condition names exactly one unjoined relation scope. It updates the
symbolic joined scope, so later field reads use that scope even when the return value is not assigned. A relation
parameter or transform input must be joined before its fields are used in a filter or projection. The legacy member form
`self.customers.lookup_join(...)` is unsupported.

The select-one `how` values are only `"left"` and `"inner"`:

- `"left"` preserves current rows and makes every joined field nullable after the join;
- `"inner"` removes current rows without a match and preserves the right field's declared nullability unless narrowed.

Use rowset helpers for `"right"`, `"full"`, and `"cross"` semantics. `hint="broadcast"` is optional and advisory; it
does not change row semantics.

### Lookup Conditions

Select-one, existence, temporal, and as-of joins use an equi-join condition: equality comparisons combined by `&`.
Each pair must compare one expression from the joined input with the current row or an earlier joined scope. The
compiler accepts either operand order, but public examples normally place the current-row expression first.

Accepted forms include:

```python
lookup_join(on=order.customer_id == customer.id, how="left")

lookup_join(
    on=(order.country == customer.country) & (order.customer_id == customer.id),
    how="left",
)

lookup_join(on=lower(trim(order.email)) == lower(trim(customer.email)), how="left")
lookup_join(on=order.external_id.null_safe_eq(customer.external_id), how="left")
```

All equality pairs in one bare call must identify the same unjoined relation. A condition that names two unjoined
relations is ambiguous and must be split into separate ordered joins.

`OR`, inequalities, non-boolean expressions, same-side comparisons, raw SQL strings, and undeclared Python functions
are rejected for select-one and existence joins. This restriction is distinct from broad rowset joins, which admit the
compileable non-equi and disjunctive predicates described above.

### Composite Keys

Composite keys are flattened from the `&` tree in source order. The ordered pairs are retained in IR, diagnostics,
traceability, generated code, and snapshots, so equivalent conditions written in a different order can produce a
different deterministic key order. Each pair must be type-compatible and must involve the same joined input.

A composite lookup is uniqueness-proven only when the exact right-side key set is known unique. Schema declarations do
not provide that proof.

### Null Semantics

Normal equality follows Spark SQL: a null on either side does not match. Null-safe equality is explicit and matches when
both sides are null:

```python
order.external_id.null_safe_eq(customer.external_id)
```

Structure never infers null-safe equality from nullable fields. Composite joins may mix normal and null-safe equality
per key pair. Diagnostics should identify which key pair uses null-safe equality.

### Case-Normalized Keys

Normalize business keys in the symbolic condition:

```python
lookup_join(
    on=lower(trim(order.email)) == lower(trim(customer.email)),
    how="left",
)
```

There is no hidden `case_insensitive=True` mode. The visible helper is part of the business key and is retained in
generated PySpark and traceability. `lower(...)` follows Spark backend behavior; it is not a promise of full Unicode
case folding or locale-specific collation.

### Lookup Cardinality Warnings

An ordinary lookup with no explicit uniqueness proof emits `JOIN-W0601` by default. Structure does not infer keys from
field names, scan data, or silently choose `first(...)` or `dropDuplicates(...)`:

```text
CompileWarning JOIN-W0601: lookup_join(...) uniqueness is not proven

Joined input:
  customers

Join key:
  order.customer_id == customers.id

Why this matters:
  Duplicate customers.id values can multiply current rows.

Use:
  add an explicit JoinDedupe policy when one selected right row is the business rule, or use inner_join(...) when
  multiplication is intended.
```

A future strict setting may promote this warning to an error. The diagnostic must remain actionable in either mode.

## Analytical Join Family

Analytical joins go beyond one-row lookup enrichment. They filter by existence, intentionally multiply rows, select
deterministic lookup winners, or select records by time. The default PySpark profile supports the following
capabilities:

```text
join.exists
join.not_exists
join.inner_join
join.lookup_dedupe
join.temporal_one
join.as_of_one
```

The analytical specification keeps the strict `lookup_join(...)` contract unchanged. Right, full, cross, non-equi, and
disjunctive broad rowset joins are owned by the full rowset support contract.

### Existence Joins

Use free predicate functions inside `where(...)`:

```python
where(exists(on=customer.id == order.customer_id))
where(not_exists(on=suppressed.email == order.email))
```

`exists(...)` keeps current rows with at least one match and has left-semi semantics. `not_exists(...)` keeps current
rows with no match and has left-anti semantics. No right-side fields are exposed, and right-side duplicates do not alter
the result. Conditions follow the select-one equality and null rules. Generated PySpark may use semi/anti joins or an
equivalent plan while preserving row count, schema, null behavior, ordering, and diagnostics.

### Row-Multiplying `inner_join(...)`

`inner_join(...)` makes multiplication intentional:

```python
inner_join(on=order_item.order_id == order.id)
```

Duplicate right rows are allowed, no lookup uniqueness warning is emitted, and the joined scope exposes right-side
fields. Choose it when the output is naturally one row per match, such as order-to-line-item expansion or a required
search or Security relationship. Output construction still decides which fields survive; right-side columns are never
implicitly appended.

The current helper supports the rowset `"inner"` and `"left"` behavior through the general rowset API. When a broad
left join is intended, prefer `left_join(...)` so the source states the preservation rule directly.

### Deterministic Lookup Dedupe

Some lookup inputs contain multiple right rows per key while the business result still requires one selected row. The
selection rule must be explicit:

```python
lookup_join(
    on=self.customer_snapshots.id == order.customer_id,
    how="left",
    dedupe=JoinDedupe.latest_by(
        self.customer_snapshots.updated_at,
        ties="error",
    ),
)
```

Initial policies are `JoinDedupe.latest_by(...)` and `JoinDedupe.earliest_by(...)`. The policy names the ordering
expression and defaults ties to `"error"`. Dedupe reduces the right input before the lookup; it must not lower to
arbitrary `first(...)` or nondeterministic `dropDuplicates(...)`.

The PySpark lowering uses `row_number()` over a window partitioned by right-side join keys, orders by the explicit
policy, keeps rank `1`, drops the temporary rank column, and then applies the lookup. Traceability retains both the
original right input and the deduped dependency. Runtime tie checks are explicit because they can add Spark work.

### Temporal Validity Lookups

`temporal_one(...)` selects a right row whose validity window contains the current-row event time:

```python
temporal_one(
    on=(promotion.tenant_id == order.tenant_id)
    & promotion.code.null_safe_eq(order.promotion_code),
    at=order.business.order_date,
    valid_from=promotion.valid_from,
    valid_to=promotion.valid_to,
    how="left",
    overlaps="error",
)
```

The default interval is closed-open: `valid_from <= at < valid_to`. A null `valid_to` is open-ended after
`valid_from`. `on=` supplies equality key pairs; `at` comes from the current row or an earlier joined scope; validity
fields come from the right input. Overlapping windows for the same right key violate the select-one contract and require
an explicit runtime check unless metadata proves non-overlap. Initial `how` values are `"left"` and `"inner"`.

This is the Structure model for slowly changing dimension type 2 lookup joins. It does not assume a storage format or
table convention.

### As-Of Lookups

`as_of_one(...)` selects a right record relative to a current-row time:

```python
as_of_one(
    on=price.symbol == trade.symbol,
    left_time=trade.trade_time,
    right_time=price.price_time,
    direction="backward",
    tolerance=duration("1 day"),
    how="left",
)
```

Initial rules are:

- `"backward"` chooses the latest right time at or before the left time;
- `"forward"` chooses the earliest right time at or after the left time;
- `"nearest"` chooses the closest non-null right time when admitted by the target;
- `tolerance=` rejects candidates farther than the supplied duration;
- `"left"` keeps unmatched rows with null right fields and `"inner"` removes them;
- ties require an explicit tie policy;
- nearest ties fail with `ties="error"`; directional nearest tie preferences remain design-gated.

## Conditions, Projection, and Scopes

### Right-Side Projection

Generated PySpark should not carry every right-side column through a join. It should select only right-side key
expressions, fields referenced by output or post-join filters, and fields needed by traceability or diagnostics when
that mode is enabled. Projection may happen before or after the physical join as long as observable semantics remain
unchanged.

### Output Construction

The output schema constructor or projection decides final fields. Structure never implicitly appends all right-side
columns. Scoped references make source collisions harmless:

```python
return OrderWithCustomer.base(order)(
    customer_id=customer.id,
)
```

For `right_join(...)` or `full_join(...)`, a current-left base constructor may be invalid because some output rows have
no left row. Use explicit projection and nullable-safe expressions:

```python
return Reconciliation.project()(
    order_id=order.id,
    customer_id=customer.id,
    matched=order.id.is_not_null() & customer.id.is_not_null(),
)
```

Right and full joins commonly use `coalesce(...)` to create a canonical identifier. Fields from nullable sides must not
be assigned to non-nullable output fields without an explicit repair or proven narrowing.

### Aliases and Joined Scopes

The current scope keeps its existing alias. The first occurrence of a right input may use its input name, and repeated
joins receive deterministic suffixes such as `customers_2`. Diagnostics identify source input and occurrence, such as
`customers#2`. Generated aliases are stable for the same source.

The compiler does not rely on Python local variable names for correctness. Symbolic scopes own field references, and
qualified references plus explicit output aliases prevent duplicate unqualified Spark columns.

`relation_alias(...)` creates a named typed occurrence for a self-join or repeated relation:

```python
reverse = relation_alias(pair, name="reverse_document")
```

The alias must be a unique non-empty Python identifier in the step. It does not execute or duplicate data.

### Join Order

Join calls execute in source order. A later join may reference the current row scope and earlier joined scopes that are
available. A filter before a join is applied before that join when its scopes are available; a filter after a join may
reference the joined scope. Output projection occurs after recorded joins and filters.

The generator may apply safe Spark-plan optimizations later, but observable row count, schema, nullability, field
collisions, and diagnostics must remain unchanged. Source order remains the reviewable semantic order.

## Hints, Strategies, and Lowering

`hint=` is advisory execution guidance. In the lookup profile, `hint="broadcast"` applies to the right side. Rowset
joins also accept `strategy=` values such as `"broadcast"`, `"shuffle_hash"`, `"shuffle_replicate_nl"`, and `"merge"`
when the selected backend capability admits them. Strategy directives are optimization hints, not semantic changes.

Generated and direct execution consume the same PySpark join recipe. Lowering rules include:

- left, inner, right, and full joins render the corresponding Spark join type;
- cross joins render `crossJoin(...)` or an equivalent target-supported operation;
- non-equi and disjunctive rowset predicates render as PySpark Column expressions;
- strategy directives render as supported hints or directives;
- the recipe follows the join with explicit projection into the declared output schema.

The compiler must not import PySpark, start Spark, inspect live data, or evaluate PySpark `Column` values while checking
or generating joins.

## IR, Explain, and Traceability

The base join IR retains the joined input and occurrence, operation kind, join type, optional hint and strategy,
ordered key pairs, equality kind per key, referenced right fields, source location or expression text, and uniqueness
status. Analytical joins add cardinality, dedupe, temporal, as-of, tie, overlap, and runtime-check policy. Rowset joins
also retain left and right scopes, predicate class, nullable sides, `allow_cartesian`, and referenced fields per side.

Cardinality classes are `pairs_only`, `preserve_left`, `preserve_right`, `preserve_both`, and `cartesian`. Predicate
classes are `equi`, `non_equi`, `disjunctive`, and `mixed`.

Explain output should show join type, predicate class, nullable sides, cardinality, field reads from each side, output
fields derived from each side, strategy directives, and relevant policies. It must not imply data-size estimates unless
a later cost model supplies measured evidence.

Traceability records both sides of rowset joins, the original and deduped dependencies for lookup dedupe, and temporal
or as-of policy fields. Source locations and normalized key pairs make diagnostics and generated artifacts
deterministic.

## Backend Capabilities and Streaming

Full rowset support requires capability checks such as:

```text
join.rowset_join
join.right_join
join.full_join
join.cross_join
join.non_equi_condition
join.disjunctive_condition
join.using_keys
```

Analytical support requires `join.exists`, `join.not_exists`, `join.inner_join`, `join.lookup_dedupe`,
`join.temporal_one`, and `join.as_of_one`. Strategy capabilities may be represented as
`join.strategy_broadcast`, `join.strategy_shuffle_hash`, `join.strategy_shuffle_replicate_nl`, and
`join.strategy_merge`.

Unsupported capabilities use `BACKEND-E2402` and identify the join form, target profile, and a supported alternative or
hook escape hatch. Semantics and capability checks remain separate: a join can be meaningful but unavailable for a
selected backend or stream mode.

Stream-static `exists(...)`, `not_exists(...)`, and `inner_join(...)` may be compatible when the current input is
streaming, the right input is static, and the selected target admits the lowered plan. Temporal joins, deduped lookups,
as-of joins, and runtime tie or overlap checks are batch-only until state, watermarks, and output-mode contracts are
specified. Right, full, cross, non-equi, and disjunctive rowset joins are batch-only in the first streaming slice.
Stream-stream joins remain out of scope unless a bounded stateful design admits them.

## Diagnostics

Join diagnostics should identify the transform, step, joined input and occurrence, join method and type, source
condition, normalized key pairs or predicate class, policy, runtime-check cost, problem, shortest valid fix, and a link
to the most specific specification.

Common diagnostics cover unsupported condition shapes, incompatible key types, ambiguous relation inference, duplicate
output names, unproven lookup uniqueness, unsupported hints or strategies, cross joins without acknowledgement,
nullable-side output errors, backend capability rejection, and invalid streaming shapes.

```text
CompileError JOIN-E2801: cross join requires explicit Cartesian acknowledgement

Join:
  CalendarExpansion.expand -> orders x calendar_days

Problem:
  A cross join can multiply every left row by every right row.

Use:
  cross_join(calendar_day, allow_cartesian=True)

See docs/dev/specifications/FullPySparkJoinSupport.md
```

```text
CompileWarning JOIN-W0601: lookup_join(...) uniqueness is not proven

Join:
  EnrichOrders.add_product -> products#1

Key:
  products.id == order.product_id

Use:
  add an explicit JoinDedupe policy, validate the source key separately, or use a rowset join when multiplication is
  intended.

See docs/dev/specifications/JoinSemantics.md
```

## Acceptance and Non-Goals

The join implementation should test source capture, key ordering, null and null-safe equality, case normalization,
deterministic aliases, right-side projection, rowset cardinality, full/right nullable-side assignment, cross guards,
non-equi and disjunctive lowering, existence duplicate behavior, lookup warnings and dedupe, temporal windows, as-of
direction and tolerance, explain output, traceability, capability rejection, and streaming classification.

Structure does not infer keys, prove uniqueness by scanning data, silently deduplicate, choose arbitrary first rows,
reorder joins automatically, promise cost-based optimization, append all right-side fields, accept raw SQL predicates,
execute storage writes after a join, support lateral or table-valued-function joins, or own stream state and
checkpoints.
