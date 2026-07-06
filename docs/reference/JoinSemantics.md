# Join Semantics

Structure joins let developers enrich a current typed row stream with fields from other named inputs while keeping the
logic symbolic, compileable, and visible to Spark's optimizer. Structure records whether a join may multiply rows,
which keys define the match, how nulls behave, which aliases own each field, and when cardinality assumptions are only
warnings rather than proven facts.

The core join model supports explicit lookup joins without implicit deduplication, implicit string column references,
or hidden data scans. Row-multiplying and existence-oriented joins are covered in
[AnalyticalJoinCoverage.md](AnalyticalJoinCoverage.md) because they change validation, traceability, and output-row
expectations.

## Public API Shape

Joins are recorded through the free-standing `lookup_join(...)` function. In the ordinary case, keep the call bare and let
the compiler infer the joined relation from the `on` clause:

```python
lookup_join(
    on=order.customer_id == customer.id,
    how=Join.LEFT,
    hint=JoinHint.BROADCAST,
)
```

When a subtransform declares the relation as a schema parameter, the relation can also be inferred:

```python
def add_customer(self, order: OrderRaw, customer: Customer) -> OrderWithCustomer:
    lookup_join(
        on=order.customer_id == customer.id,
        how=Join.LEFT,
    )
    return OrderWithCustomer.base(order)(customer_name=customer.name)
```

Inference is valid only when the `on` condition references exactly one unjoined relation scope. If the condition
references no unjoined relation or more than one, the inferred form is ambiguous and must be rewritten before the
compiler can accept it.

For relation parameters and transform input scopes, `lookup_join(on=...)` updates the symbolic relation proxy, so later
field reads use the joined scope. A relation parameter cannot be used in a filter or projection until it has been
joined.

The old member spelling `self.customers.lookup_join(...)` is rejected. Use the free-standing bare form instead.

Canonical function:

- `lookup_join(*, on, how, hint=None)`: an inferred lookup join.
- Legacy explicit-selection overloads remain supported, but they are not the documented style.

Rules:

- `on` is required.
- `how` is required. Source should show whether unmatched rows are kept or removed.
- `hint` is optional and advisory.
- `lookup_join(...)` records the same ordered join operation whether the relation is inferred or explicit.
- `lookup_join(...)` returns a relation proxy whose fields read from the joined scope, such as `customer.name`.
- For schema relation parameters and cached transform input scopes, later field reads from the same proxy use the joined
  scope after `lookup_join(...)`, even when the return value is not assigned.

## Join Types

The compiled DSL supports:

- `Join.LEFT`: keep every current row; right fields are null when no match exists.
- `Join.INNER`: keep only current rows that have at least one right match.

`Join.RIGHT`, `Join.FULL`, `Join.CROSS`, and semi/anti joins are deferred. They do not fit the v1 row-centric schema
constructor cleanly because they can introduce rows that do not have a current-row source, or they return existence
semantics rather than a joined right scope. The v2+ plan for semi/anti predicates lives in
[AnalyticalJoinCoverage.md](AnalyticalJoinCoverage.md).

If the public enum exposes deferred values for forward compatibility, the compileability checker must reject them in
compiled subtransforms with a diagnostic that names the supported v1 values.

## Join Conditions

The v1 join condition is an equi-join condition: a boolean expression made from equality comparisons joined by logical
AND.

Accepted:

```python
lookup_join(on=order.customer_id == customer.id)

lookup_join(on=(order.country == customer.country) & (order.customer_id == customer.id))

lookup_join(on=lower(trim(order.email)) == lower(trim(customer.email)))

lookup_join(on=order.customer_external_id.null_safe_eq(customer.external_id))
```

Rejected in v1:

- `OR` conditions.
- Inequality conditions such as `<`, `<=`, `>`, or `>=`.
- Non-boolean `on` expressions.
- Comparisons where neither side references the joined input.
- Conditions that compare two fields from the same side.
- Python string column paths such as `"customers.id = orders.customer_id"`.
- User Python functions unless they are declared as compileable expression helpers.

Each equality comparison contributes one key pair. In a pair, one expression must reference the joined input scope and
the other expression must reference the current row scope or a previously joined scope. The compiler accepts either
operand order. Public examples place the current-row expression on the left and the joined-input expression on the
right because that reads naturally for `Join.LEFT` enrichment steps.

In the bare form, all equality pairs in one `lookup_join(...)` call must point to the same unjoined relation. A predicate
that mentions two unjoined relations is ambiguous and must be split into separate joins.

## Composite Keys

Composite joins are expressed by combining equality pairs with `&`:

```python
lookup_join(
    on=(order.country == customer.country) & (order.customer_id == customer.id),
    how=Join.LEFT,
)
```

The key order in IR must follow source order after flattening the `&` tree from left to right. This makes diagnostics,
traceability, generated code, and snapshot tests deterministic.

Composite key rules:

- All key pairs must be compileable expressions.
- All key pairs must involve the same joined input scope for a single join call.
- A key pair may compare expression helpers, not only bare fields, when the helpers are deterministic and row-local.
- Type compatibility follows the nullability and type coercion reference.
- A composite `lookup_join(...)` is uniqueness-proven only when the exact right-side key set is known unique.

## Null Semantics

Normal equality uses Spark SQL equality. If either side is null, the comparison does not match.

Null-safe equality is explicit:

```python
order.customer_id.null_safe_eq(customer.id)
```

This lowers to Spark's null-safe equality operation. It matches when both sides are null.

Rules:

- Structure must not infer null-safe equality from nullable fields.
- Composite joins may mix normal equality and null-safe equality per key pair.
- Diagnostics must name which key pair is null-safe when explaining generated join conditions.
- For `Join.LEFT`, every field from the joined scope is nullable after the join, even if the right schema declares the
  field as non-nullable.
- For `Join.INNER`, joined fields keep their declared nullability unless the join condition or a filter narrows them.

## Case-Normalized Keys

Case normalization is expressed in the join condition with compileable expression helpers:

```python
lookup_join(
    on=lower(trim(order.email)) == lower(trim(customer.email)),
    how=Join.LEFT,
)
```

There is no v1 `case_insensitive=True` join option. Normalization belongs in the expression because it is part of the
business key. Keeping it visible makes generated PySpark and traceability reviewable.

The v1 `lower(...)` helper follows Spark's backend behavior. It is not a promise of full Unicode case folding or
locale-specific collation. If the project later adds richer collation semantics, that should be a separate expression
helper or configuration contract.

## `lookup_join(...)` Cardinality

`lookup_join(...)` means that each current row should match zero or one right-side row. It is valid for many current rows
to match the same right row. In relational terms, it covers many-to-one and one-to-one lookup joins.

Duplicate right-side rows for the chosen key are a contract violation for `lookup_join(...)` because Spark would multiply
the current row. Structure must not silently deduplicate or choose an arbitrary first row.

Uniqueness proof sources:

- A right-side schema field marked `primary_key=True` when the join key is exactly that field.
- A future unique-key metadata feature when the join key exactly matches one declared unique key.
- A user-enabled runtime uniqueness check, if implemented later.

When no uniqueness proof exists, v1 should compile with a warning by default:

```text
CompileWarning JOIN-W0601: lookup_join(...) uniqueness is not proven

Joined input:
  customers

Join key:
  order.customer_id == customers.id

Why this matters:
  If customers has duplicate id values, this join can multiply rows.

Use:
  mark Customer.id as primary_key=True, declare a unique key, use inner_join(...) if multiplication is intended,
  or add an explicit JoinDedupe policy when one selected right row is the business rule.
```

Projects may later add a strict setting that turns this warning into an error. That setting is not required for the v1
semantics, but diagnostics should be designed so the promotion is straightforward.

## `inner_join(...)` Cardinality

`inner_join(...)` (v2) means row multiplication is intentional. If one current row matches three right rows, the
downstream step sees three rows. Detailed v2+ behavior is owned by
[AnalyticalJoinCoverage.md](AnalyticalJoinCoverage.md).

Rules:

- Duplicate right-side rows are allowed.
- No uniqueness warning is emitted.
- `Join.LEFT` preserves unmatched current rows with null joined fields.
- `Join.INNER` removes current rows with no right match.
- Output schema construction still decides which fields survive; right-side columns are not implicitly appended.

Developers should choose `inner_join(...)` when the output is naturally one row per match, such as order-to-line-item
expansion. The default PySpark profile now lowers this shape as an explicit row-multiplying join, so it no longer needs
to hide in a hook.

## Right-Side Projection

Generated PySpark must not carry all right-side columns through the join by default. The generator should select only:

- right-side key expressions needed by the join condition;
- right-side fields referenced by output projection;
- right-side fields referenced by post-join filters;
- right-side fields needed by traceability or diagnostics when that mode is enabled.

Projection may happen before or after the physical join as long as observable semantics remain the same. The generated
code should avoid duplicate unqualified column names by aliasing and explicit `select(...)`.

## Aliases and Joined Scopes

A joined scope is the symbolic object returned by `lookup_join(...)`. It owns field references from the right side of that
join. v2 extends the same idea to `inner_join(...)`.

Alias rules:

- The current row scope keeps its existing alias.
- The first join of an input may use the input name as the generated DataFrame alias, such as `customers`.
- Repeated joins of the same input in one subtransform must receive deterministic suffixes, such as `customers_2`.
- Diagnostics should refer to the source input name and join occurrence when needed, for example `customers#2`.
- Generated aliases must be stable across runs for the same source.

The compiler does not rely on Python local variable names for correctness. The local name is useful to the developer
but may not be available after symbolic execution without source analysis.

## Field Name Collisions

Field names are never resolved by unqualified strings after a join. Developers refer to fields through symbolic scopes:

```python
return OrderWithCustomer.base(order)(
    customer_id=customer.id,
)
```

Rules:

- A collision between `order.id` and `customer.id` is harmless while expressions remain scoped.
- The output schema constructor or schema base overlay determines final output field names.
- A duplicate output field name is a schema construction issue, not a join issue.
- Generated PySpark must use qualified column references and explicit aliases.
- Structure must not implicitly append right-side columns to the output.

## Join Order

Join calls inside one subtransform execute in source order. A later join can reference the current row scope and any
previously joined scope that is still in scope.

Filters obey source order:

- A `where(...)` recorded before a join is applied before that join when it references only available scopes.
- A `where(...)` recorded after a join may reference that joined scope and is applied after the join.
- Projection into the returned output schema happens after recorded joins and filters for the subtransform.

The generator may perform safe Spark-plan optimizations later, but v1 should preserve source order in generated code
because it is easier to review and debug.

## Broadcast Hints

`hint=JoinHint.BROADCAST` applies to the joined right input in v1:

```python
lookup_join(
    on=order.customer_id == customer.id,
    how=Join.LEFT,
    hint=JoinHint.BROADCAST,
)
```

Generated PySpark may lower this to `F.broadcast(right_df)` or to a Spark hint on the right DataFrame. The hint is
advisory. It must not change row semantics.

Rules:

- Broadcast hints apply to the right side only in v1.
- Unsupported hints must be rejected or warned by backend capability checks.
- Streaming compatibility checks must reject hints or join shapes that Spark cannot run for the configured streaming
  mode.
- Future join strategy hints belong in the optimization roadmap, not in the v1 semantic core.

## IR Contract

The join IR should preserve:

- joined input scope;
- joined scope occurrence;
- operation kind, including `lookup_join`, row-filtering existence joins, and row-multiplying `inner_join`;
- join type;
- optional hint;
- optional strategy hint for row-multiplying joins;
- optional deterministic dedupe policy for lookup joins;
- ordered key pairs;
- equality kind per key pair, either normal equality or null-safe equality;
- referenced right fields;
- source location or diagnostic expression text when available;
- whether right-side uniqueness is proven, unproven, or explicitly unchecked.

The compileability checker consumes this IR to validate boolean conditions, supported join types, key compatibility,
alias uniqueness, and `lookup_join(...)` uniqueness warnings.

## Diagnostics

Examples:

```text
CompileError JOIN-E0601: Unsupported join condition

Join:
  EnrichOrders.add_customer -> customers#1

Source condition:
  (customers.country == order.country) | (customers.id == order.customer_id)

Problem:
  v1 joins support equality key pairs combined with AND. OR conditions are not compileable.

Use:
  split the logic into separate subtransforms or move custom join logic into an @after hook.

See docs/reference/JoinSemantics.md
```

```text
CompileWarning JOIN-W0601: lookup_join(...) uniqueness is not proven

Join:
  EnrichOrders.add_customer -> customers#1

Key:
  customers.id == order.customer_id

Use:
  field(String(), primary_key=True) on Customer.id, declare a unique key, or wait for v2 inner_join(...).

See docs/reference/JoinSemantics.md
```
