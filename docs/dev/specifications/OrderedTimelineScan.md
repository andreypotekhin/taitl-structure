# Ordered Timeline Scan

## Purpose

`scan(...)` expresses a bounded recurrence over a caller-supplied, finite, ordered timeline. It is for cases where each
output row depends on state produced by earlier rows in the same partition, such as Fibonacci-like state progression.
It is not a replacement for `lag(...)`: `lag(...)` reads values that already exist in input rows, while `scan(...)`
carries explicit typed state from one ordered row to the next.

The first release is a PySpark-plugin, batch-only contract. It does not create timeline rows, persist state between
invocations, start streaming queries, or hide Python execution behind a typed-looking helper.

## Public Form

`scan(...)` is exported from `structure.plugin.pyspark`:

```python
scan(
    *,
    initial: StateSchema,
    partition_by: Expression | tuple[Expression, ...],
    order_by: Expression | tuple[Expression, ...],
    max_rows: int,
    step: Callable[[StateSchema, TimelineRow], StateSchema],
    ties: TiePolicy = TiePolicy.ERROR,
) -> StateSchema
```

`initial` is a fully populated `Schema` instance. `step` receives a symbolic state scope and the current timeline row,
then returns exactly the same state schema. The result of `scan(...)` is the state before the transition for the
current timeline row. Output projections can read that returned state just like any other symbolic schema value.

## Example

```python
class Tick(Schema):
    series = string(nullable=False)
    index = long(nullable=False)


class FibonacciState(Schema):
    previous = long(nullable=False)
    current = long(nullable=False)


class Fibonacci(Schema):
    series = string(nullable=False)
    index = long(nullable=False)
    value = long(nullable=False)


class FibonacciFromTimeline(Transform):
    ticks = input(Tick)
    values = output(Fibonacci)

    def calculate(self, tick: Tick) -> Fibonacci:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=tick.series,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current,
            ),
        )
        return Fibonacci(series=tick.series, index=tick.index, value=state.previous)
```

For each `series`, timeline indices `0..9` produce values `0, 1, 1, 2, 3, 5, 8, 13, 21, 34`. A second series starts
from the same initial state. Missing indices are allowed because recurrence order, not contiguous numbering, defines
the contract.

## Semantics

- The input timeline is the active rowset of the containing step.
- `partition_by` and `order_by` are required and must contain at least one expression each.
- `max_rows` is a positive integer literal and applies independently to each partition.
- The first release accepts only ascending order expressions with `TiePolicy.ERROR`.
- Equal order keys inside one partition fail at Spark evaluation time with a registered scan diagnostic.
- Null partition keys are allowed; null order keys fail because they make the recurrence order ambiguous.
- Empty input yields an empty output relation with the declared output schema.
- Each nonempty partition starts from the same `initial` state.
- The scan state is visible only as state-before-transition for the current row.
- The transition callback may read the state scope and current timeline row, but must produce symbolic Structure
  expressions only.

## Rejected Forms

The compiler rejects:

- omitted or empty `partition_by` / `order_by`;
- nonliteral or nonpositive `max_rows`;
- non-`Schema` initial state or partially populated initial state;
- callback arity other than `(state, row)`;
- callback return values that are not the same state schema;
- missing, extra, wrong-type, or nullability-incompatible state fields;
- multiple different scans in one step;
- nested scans or scans inside a scan callback;
- scans in filters, joins, aggregate keys, hooks, or unrelated relation-operation arguments;
- scans after a join, aggregation, row-expanding operation, set-composition operation, or order-destroying operation;
- streaming inputs and Spark Connect profiles that lack the required public PySpark functions;
- UDF, Pandas, RDD, Spark action, raw hook, or driver-loop recurrence fallback.

## Lowering

The PySpark plugin lowers the scan as an ordinary public DataFrame/Column plan:

1. Preserve each input row as a payload struct.
2. Group by the declared partition expressions.
3. Collect payloads, sort by declared order expressions, and validate duplicate/null order keys plus `max_rows`.
4. Use Spark higher-order `aggregate(...)` to fold the ordered payload array from the initial state.
5. Accumulate output payload plus state-before-transition rows.
6. Expand accumulated rows with the same public row-expansion family admitted for Sprint 25.
7. Project the user's declared output schema through the normal schema-validation path.

Generated and online execution consume the same immutable recipe. Generated code must expose readable grouped, folded,
and expanded intermediate frames and must not re-run the user callback at runtime.

## Capability and Diagnostics

The capability id is `pyspark.ordered_timeline_scan`. The feature is batch-only for ordinary PySpark until the target
capability matrix proves otherwise.

Diagnostics must cover invalid public arguments, invalid callback/state shape, unsupported placement, duplicate order
keys, null order keys, partition-size overrun, streaming rejection, and missing target capability. Each diagnostic
should explain when `lag(...)` is appropriate and when `scan(...)` is required.

## Evidence

Implementation must add:

- source-level symbolic tests for state scope capture and rejection cases;
- recipe, capability, explain, and traceability tests;
- generated-source tests proving public PySpark functions and no UDF/RDD/Pandas/action/driver-loop tokens;
- online/generated parity tests for two-series Fibonacci, empty input, one-row partitions, missing index gaps,
  duplicate keys, null order keys, partition overrun, and transition reads from the current row;
- live PySpark evidence for the supported batch target range when the environment is available.
