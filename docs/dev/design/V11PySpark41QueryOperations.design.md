# V11 PySpark 4.1 Query Operations Design

## Purpose

This design defines typed, compiler-visible relation queries for Spark 4.1. A transform can test an inner query with
native `exists`, use an uncorrelated inner query for native `isin`, or join each driving row to zero or more rows from
a correlated lateral query. Structure keeps the inner relation's schema and operations visible to validation,
generation, and online execution; it does not accept arbitrary DataFrame callbacks.

New query consumers are batch-only and admitted for exact ordinary PySpark and Spark Connect `>=4.1,<4.2` profiles.
Focused online/generated/native parity passed independently on Spark/PySpark 4.1.0 for both variants. The project
default profile remains `>=3.5,<4.1`.

## Relation query contract

An additional schema parameter in a step is an unjoined named input relation. Its `.where(...)` and `.filter(...)`
methods build an immutable query over that relation. Query descriptors also support `.project(SchemaOrFieldNames)`,
`.order_by(...)`, `.limit(n)`, and `.exists()`. Projection uses declared fields and their schema metadata. Query
construction does not mutate the driving lane or execute a Spark action. Query intermediates are considered consumed
when their final descriptor is consumed; an entirely unused query receives an actionable compile diagnostic.

Existing `.where(...)` on a driving or already joined row scope continues to filter the current step. The global
`where(...)` helper also retains its existing step-filter meaning. `.filter(...)` is an alias for `.where(...)` in both
contexts.

The initial query body supports one named input occurrence, filters, projection, ordering, and limits. It does not
support query-local joins, aggregation or windows, nested subqueries, correlated IN, or arbitrary DataFrame callbacks.
Two parameters with the same schema are distinct relation occurrences. Use `relation_alias(...)` to name separate
occurrences; field names alone never resolve a scope.

## EXISTS and IN

Calling `.exists()` on a bare relation or prepared query returns a native-shaped, non-null Boolean expression. It can
be projected, filtered, negated with `~`, and combined with `&` or `|`. Existing `exists(..., on=...)` and
`not_exists(..., on=...)` remain filtering semi/anti joins and preserve their current behavior and streaming contract.

Calling `expression.isin(query)` or `expression.isin(relation)` builds native IN-subquery semantics. Existing literal
and list forms remain unchanged. The right side must be uncorrelated and project one column for a scalar expression.
Struct expressions such as `named_struct(...)` may compare against multiple projected columns in their declared order.
Validate arity, scalar comparison compatibility, and structural member compatibility without relying on schema class
identity or column names.

IN and NOT IN preserve the native PySpark `Column.isin(DataFrame)` behavior for null right-side values and empty
relations. Do not implement IN using ordinary or null-safe semi-join keys. Native Spark evaluation is the reference
for nested nullable members and empty-input behavior.

## Correlation and outer references

Inside an inner query, `.outer()` marks a field expression as coming from the visible driving row or a previously
joined outer scope. The marker is required only inside the inner query. A lateral join's `on=` expression instead uses
normal field references for the driving and projected inner rows.

Reject unmarked outer capture, unrelated scopes, inner fields inside `.outer()`, outer markers outside an inner query,
nested correlation, ambiguous inferred inner relations, and references to projected-away fields. Diagnostics name the
offending scope and suggest an explicit relation, alias, projection, or `.outer()` marker.

## Lateral joins

The public shape is:

    lateral_join(
        relation=None,
        *,
        where=None,
        on=None,
        to=None,
        order_by=(),
        limit=None,
        how="inner",
        allow_cartesian=False,
    )

Use `lateral_join(prepared_query, on=..., how=...)` for a reusable query, or omit `relation` to infer one inner input
from `where` and `on`. Inference must find exactly one candidate. Inline options append operations in the order WHERE,
ORDER BY, LIMIT, PROJECT. The `on=` predicate remains separate and is evaluated after those query operations. This is
observable when a limited query is followed by a join condition: `on` must never be pushed before the limit.

The driving lane is implicit. `lateral_join` returns a joined row scope and also binds the projected fields to their
original right input scope, so assigning the result is optional. Preserve right duplicates and multiple matches. Inner
joins drop driving rows with no result. Left joins retain one null-extended right row when there are no matches, and
make every right field nullable in the joined scope. Accept only inner and left, including the documented left aliases.

`.limit(n)` requires a preceding `.order_by(...)` and a non-negative integer literal that is not a Boolean; zero is
valid. Ordering determines which rows survive a limit but does not guarantee final joined output order. A limit gives
metadata bounds of 0..N for inner and 1..max(1,N) for left. No count action or singleton assertion is introduced.

If the query has no outer dependency and `on` has no condition connecting its output to an outer scope, require
`allow_cartesian=True`. This checks scope connectivity; it does not attempt to prove that a predicate is selective.

## Evidence

The accepted focused fixtures run through native Spark, Structure online execution, and generated PySpark on ordinary
and Connect Spark/PySpark 4.1.0. Both variants pass correlated EXISTS, scalar and composite IN with nullable source
values, and a correlated ordered-limit left lateral query with duplicate matches, post-limit `on=`, missing matches,
schema parity, and generated-source safety scans. Spark-free tests cover zero bounds, scope validation, rejected
correlation in IN, Cartesian opt-in, and profile rejection. Generated code uses public PySpark APIs and has no
`._jdf`, `._jvm`, `.rdd`, or `.sparkContext` access. Future backend/profile claims require their own parity runs.
