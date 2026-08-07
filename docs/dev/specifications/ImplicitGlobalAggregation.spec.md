# Implicit Global Aggregation

## Purpose

Structure must preserve the existing ability to construct a result from aggregate expressions without calling
`group_by(...)`. This specification calls that shape an implicit global aggregate: all rows from the current relation
form one aggregate group.

The feature exists to express whole-input summaries naturally and is required by the Search index summary workflow. It
is not replaced by, and does not require, a `group_all()` helper.

## Source Contract

An aggregate-only step may return a schema directly:

```python
class Summary(Transform):
    rows = input(Row)
    summary = output(RowSummary)

    def summarize(self, row: Row) -> RowSummary:
        return RowSummary(total=count(), amount=sum(row.amount))
```

No preceding `group_by(...)`, `rollup(...)`, `cube(...)`, or `grouping_sets(...)` call is required. Calling a grouping
operation retains its existing keyed/subtotal semantics and is not reinterpreted as this feature.

## Validity Rules

- Every output expression must be an aggregate expression or a literal that is valid independently of an input row.
- A current-row field, row-local expression, join field, or window expression is invalid in an implicit global output
  unless it is itself nested under an admitted aggregate expression.
- Filtered aggregate metrics remain valid and apply their predicate before the global aggregation.
- Multiple aggregate metrics share one global aggregate operation.
- Existing type, nullability, aggregate-use, and source-location diagnostics remain applicable.

## Empty Input

The result cardinality is exactly one row for an empty input when every output expression is a valid Spark global
aggregate or literal. The individual values use normal Spark aggregate semantics and their declared Structure
nullability must be honest. For example, `count()` is zero, while a nullable `sum(...)` may be null.

An output requiring a current row is rejected at symbolic validation, before execution, rather than yielding an
arbitrary row or a silently empty result.

## Implementation Contract

Symbolic result construction marks an aggregate output with no grouping record as global; it must not synthesize a
fake grouping key or a public helper call. The immutable PySpark recipe has an empty key sequence and one aggregate
assignment sequence. Online execution and generated code lower the same recipe through a public Spark aggregate plan.

Explain and traceability state `global aggregate`, cardinality `one row`, and the referenced source fields. Streaming
classification follows the existing aggregate capability rules; this specification does not grant new streaming
support.

## Acceptance

- An aggregate-only transform compiles without a `group_by(...)` call.
- Empty input returns one declared summary row with normal aggregate values.
- A mixed current-row and aggregate output fails with a registered aggregate-use diagnostic.
- Online and generated PySpark return the same rows/schema, and generated source contains no action, UDF fallback, or
  synthetic grouping column.
