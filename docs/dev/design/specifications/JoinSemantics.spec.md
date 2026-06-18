# Join Semantics Specification

## Status

Ready for implementation.

This specification resolves `Challenges.md` C9, "Join Semantics Need Sharper Definitions".

## Purpose

Structure joins let developers enrich a current typed row stream with fields from other named inputs while keeping the
logic symbolic, compileable, and visible to Spark's optimizer. The compiler must know whether a join may multiply rows,
which keys define the match, how nulls behave, which aliases own each field, and when cardinality assumptions are only
warnings rather than proven facts.

The v1 goal is deliberately narrow: support explicit lookup joins and explicit row-multiplying joins without implicit
deduplication, implicit string column references, or hidden data scans.

## Public API Shape

Join methods are called from an input scope declared with `input(Schema)`:

```python
customer = self.customers.join_one(
    on=self.customers.id == order.customer_id,
    how=Join.LEFT,
    hint=JoinHint.BROADCAST,
)
```

Canonical v1 methods:

- `join_one(*, on, how, hint=None)`: a lookup join that promises at most one right-side row per current row.
- `join_many(*, on, how, hint=None)`: a cardinality-expanding join that may produce multiple output rows per current
  row.

Rules:

- `on` is required.
- `how` is required in v1. Source should show whether unmatched rows are kept or removed.
- `hint` is optional and advisory.
- `join_one(...)` and `join_many(...)` return a joined symbolic scope. Fields are read from that scope, such as
  `customer.name`.

## Join Types

The v1 compiled DSL supports:

- `Join.LEFT`: keep every current row; right fields are null when no match exists.
- `Join.INNER`: keep only current rows that have at least one right match.

`Join.RIGHT`, `Join.FULL`, `Join.CROSS`, and semi/anti joins are deferred. They do not fit the v1 row-centric schema
constructor cleanly because they can introduce rows that do not have a current-row source, or they return existence
semantics rather than a joined right scope.

If the public enum exposes deferred values for forward compatibility, the compileability checker must reject them in
compiled subtransforms with a diagnostic that names the supported v1 values.

## Join Conditions

The v1 join condition is an equi-join condition: a boolean expression made from equality comparisons joined by logical
AND.

Accepted:

```python
self.customers.id == order.customer_id

(self.customers.country == order.country) & (self.customers.id == order.customer_id)

lower(trim(self.customers.email)) == lower(trim(order.email))

self.customers.external_id.null_safe_eq(order.customer_external_id)
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
the other expression must reference the current row scope or a previously joined scope. The compiler may normalize
operand order internally.

## Composite Keys

Composite joins are expressed by combining equality pairs with `&`:

```python
customer = self.customers.join_one(
    on=(self.customers.country == order.country) & (self.customers.id == order.customer_id),
    how=Join.LEFT,
)
```

The key order in IR must follow source order after flattening the `&` tree from left to right. This makes diagnostics,
lineage, generated code, and snapshot tests deterministic.

Composite key rules:

- All key pairs must be compileable expressions.
- All key pairs must involve the same joined input scope for a single join call.
- A key pair may compare expression helpers, not only bare fields, when the helpers are deterministic and row-local.
- Type compatibility follows the nullability and type coercion specification.
- A composite `join_one(...)` is uniqueness-proven only when the exact right-side key set is known unique.

## Null Semantics

Normal equality uses Spark SQL equality. If either side is null, the comparison does not match.

Null-safe equality is explicit:

```python
self.customers.id.null_safe_eq(order.customer_id)
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
customer = self.customers.join_one(
    on=lower(trim(self.customers.email)) == lower(trim(order.email)),
    how=Join.LEFT,
)
```

There is no v1 `case_insensitive=True` join option. Normalization belongs in the expression because it is part of the
business key. Keeping it visible makes generated PySpark and lineage reviewable.

The v1 `lower(...)` helper follows Spark's backend behavior. It is not a promise of full Unicode case folding or
locale-specific collation. If the project later adds richer collation semantics, that should be a separate expression
helper or configuration contract.

## `join_one(...)` Cardinality

`join_one(...)` means that each current row should match zero or one right-side row. It is valid for many current rows
to match the same right row. In relational terms, it covers many-to-one and one-to-one lookup joins.

Duplicate right-side rows for the chosen key are a contract violation for `join_one(...)` because Spark would multiply
the current row. Structure must not silently deduplicate or choose an arbitrary first row.

Uniqueness proof sources:

- A right-side schema field marked `primary_key=True` when the join key is exactly that field.
- A future unique-key metadata feature when the join key exactly matches one declared unique key.
- A user-enabled runtime uniqueness check, if implemented later.

When no uniqueness proof exists, v1 should compile with a warning by default:

```text
CompileWarning STRUCT-W3xxx: join_one(...) uniqueness is not proven

Joined input:
  customers

Join key:
  customers.id == order.customer_id

Why this matters:
  If customers has duplicate id values, this join can multiply rows.

Use:
  mark Customer.id as primary_key=True, declare a unique key, or use join_many(...) if multiplication is intended.
```

Projects may later add a strict setting that turns this warning into an error. That setting is not required for the v1
semantics, but diagnostics should be designed so the promotion is straightforward.

## `join_many(...)` Cardinality

`join_many(...)` means row multiplication is intentional. If one current row matches three right rows, the downstream
step sees three rows.

Rules:

- Duplicate right-side rows are allowed.
- No uniqueness warning is emitted.
- `Join.LEFT` preserves unmatched current rows with null joined fields.
- `Join.INNER` removes current rows with no right match.
- Output schema construction still decides which fields survive; right-side columns are not implicitly appended.

Developers should choose `join_many(...)` when the output is naturally one row per match, such as order-to-line-item
expansion.

## Right-Side Projection

Generated PySpark must not carry all right-side columns through the join by default. The generator should select only:

- right-side key expressions needed by the join condition;
- right-side fields referenced by output projection;
- right-side fields referenced by post-join filters;
- right-side fields needed by lineage or diagnostics when that mode is enabled.

Projection may happen before or after the physical join as long as observable semantics remain the same. The generated
code should avoid duplicate unqualified column names by aliasing and explicit `select(...)`.

## Aliases and Joined Scopes

A joined scope is the symbolic object returned by `join_one(...)` or `join_many(...)`. It owns field references from the
right side of that join.

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
return OrderWithCustomer(
    id=order.id,
    customer_id=customer.id,
)
```

Rules:

- A collision between `order.id` and `customer.id` is harmless while expressions remain scoped.
- The output schema constructor determines final output field names.
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
customer = self.customers.join_one(
    on=self.customers.id == order.customer_id,
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
- method kind, either `join_one` or `join_many`;
- join type;
- optional hint;
- ordered key pairs;
- equality kind per key pair, either normal equality or null-safe equality;
- referenced right fields;
- source location or diagnostic expression text when available;
- whether right-side uniqueness is proven, unproven, or explicitly unchecked.

The compileability checker consumes this IR to validate boolean conditions, supported join types, key compatibility,
alias uniqueness, and `join_one(...)` uniqueness warnings.

## Diagnostics

Join diagnostics should include:

- transform class;
- subtransform method;
- joined input;
- join occurrence or generated alias;
- join method, join type, and hint;
- source condition;
- normalized key pairs;
- problem;
- suggested DSL fix;
- link to this specification.

Examples:

```text
CompileError STRUCT-E3xxx: Unsupported join condition

Join:
  EnrichOrders.add_customer -> customers#1

Source condition:
  (customers.country == order.country) | (customers.id == order.customer_id)

Problem:
  v1 joins support equality key pairs combined with AND. OR conditions are not compileable.

Use:
  split the logic into separate subtransforms or move custom join logic into an @after hook.

See docs/dev/design/specifications/JoinSemantics.spec.md
```

```text
CompileWarning STRUCT-W3xxx: join_one(...) uniqueness is not proven

Join:
  EnrichOrders.add_customer -> customers#1

Key:
  customers.id == order.customer_id

Use:
  field(String(), primary_key=True) on Customer.id, declare a unique key, or use join_many(...).

See docs/dev/design/specifications/JoinSemantics.spec.md
```

## Acceptance Scenarios

The implementation is complete when tests prove these scenarios:

- A single-field `join_one(...)` with a right primary key compiles without a uniqueness warning.
- A composite `join_one(...)` preserves key order and generates an AND condition.
- A nullable normal-equality key does not match nulls.
- A nullable null-safe key lowers to Spark null-safe equality.
- A case-normalized join key lowers expression helpers on both sides.
- An unproven `join_one(...)` emits a uniqueness warning and does not deduplicate.
- `join_many(...)` allows row multiplication without a uniqueness warning.
- A left join makes joined fields nullable in output type checks.
- A post-join `where(joined.id.is_not_null())` can require a match after a left join.
- Repeated joins of the same input produce deterministic generated aliases.
- Right-side projection includes referenced fields and avoids carrying unused columns.
- Unsupported join types and unsupported conditions fail with actionable diagnostics.
