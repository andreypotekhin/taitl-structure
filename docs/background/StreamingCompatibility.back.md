# Streaming Compatibility

Structure generates PySpark DataFrame transforms. The current streaming compatibility contract does not generate Spark
Structured Streaming jobs. A generated transform is streaming-compatible when a caller can pass a streaming DataFrame as
the current pipeline input and Spark can analyze the resulting DataFrame plan without Structure adding unsupported
streaming operations, actions, stateful streaming features, or streaming lifecycle code.

The streaming-compatible contract keeps lifecycle ownership with the caller. Row-local
projection, row-local filtering, schema-only validation, stream-static joins and left-semi filtering,
transform-scoped watermarks, event-time and session-window aggregations, watermarked dedupe, and admitted bounded
stream-stream joins are in scope. Triggers, checkpoints, streaming sources, streaming sinks, query start, query stop,
deployment, and recovery are outside this compatibility contract and remain caller-owned.

See the exhaustive [streaming API table](../api/Streaming.api.md) for supported declarations, parity, and examples.

## Definition

Streaming compatibility means all of these are true:

- Generated code accepts ordinary PySpark `DataFrame` objects and does not require a batch-only DataFrame.
- The current pipeline DataFrame may be streaming.
- Side input DataFrames used for joins are treated as static batch DataFrames unless declared
  `streaming=True`.
- Generated operations are supported by Spark Structured Streaming for that runtime shape.
- Generated code does not call Spark actions such as `collect()`, `count()`, `toPandas()`, or `show()`.
- Generated code does not create `readStream`, call `writeStream`, set triggers, set checkpoints, or start queries.
- Opaque hooks are absent or explicitly marked streaming-safe.

Streaming compatibility does not mean Structure starts a streaming query. Structure checks the transformation contract
at compile time, reports required output modes where relevant, and leaves query lifecycle choices to the caller-owned
shape.

When transforms are composed, streaming lineage is carried by outputs produced from inputs declared with
`streaming=True`. The default composition policy rejects a streaming output assigned to a downstream input without the
same declaration. Set `allow_stream_to_batch = true` for an intentional undeclared boundary. An explicit
`streaming=False` on the downstream input or transform is a hard rejection because it states that streaming is not
supported.

## Runtime Shape

The streaming-compatible runtime shape is one streaming current pipeline DataFrame plus zero or more static side
inputs.

Example:

```python
orders = spark.readStream.table("orders")
customers = spark.read.table("customers")

result = EnrichOrdersGenerated(spark=spark).run(
    orders=orders,
    customers=customers,
)

query = result.writeStream \
    .option("checkpointLocation", checkpoint) \
    .toTable("orders_enriched")
```

Rules:

- The current pipeline DataFrame is the DataFrame flowing through the source-ordered step-method chain.
- Additional named inputs referenced through joins are static side inputs unless declared `streaming=True`.
- Passing a streaming DataFrame as a joined side input requires explicit `streaming=True`, watermarks on both
  sides, and an event-time bound for the admitted bounded stream-stream join shapes.
- Generated code does not branch on `df.isStreaming` except for ordinary `drop_duplicates(...)`: that narrowly scoped
  branch selects batch `dropDuplicates` or streaming `dropDuplicatesWithinWatermark`. It owns no lifecycle behavior.

## Configuration

The seed configuration includes:

```toml
streaming_compatibility_checks = true
```

When `streaming_compatibility_checks = true`, `structure check` and `structure compile` run a streaming compatibility
pass over the TransformPlan IR.

Transform-level opt-in uses:

```python
@transform(streaming=True)
class EnrichOrders(Transform):
    enriched = output(OrderEnriched)
    ...
```

Severity rules:

- If `streaming_compatibility_checks = false`, Structure emits no streaming compatibility diagnostics.
- If checks are enabled and a transform does not opt in with `streaming=True`, incompatible operations emit
  warnings.
- If checks are enabled and a transform opts in with `streaming=True`, incompatible or unknown operations
  emit errors.
- If checks are disabled and a transform opts in with `streaming=True`, the transform-level marker wins and
  Structure still runs the compatibility pass for that transform.

This gives ordinary batch projects useful visibility without making every future batch-only operation fail, while still
letting streaming-bound transforms enforce the contract in CI.

## Supported v1 Operations

Projection is compatible when every projected value is a compileable, row-local Spark Column expression:

```python
return OrderClean(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Filtering is compatible when each predicate is a compileable, row-local boolean Spark Column expression:

```python
where(order.id.is_not_null())
where(to_decimal(order.total, precision=12, scale=2) >= 0)
```

Expression-based derived columns are compatible when they lower to Spark SQL functions, Column operators, or scalar
`@special(type="udf")` calls that do not require cross-row state, local collection, or RDD conversion.

Schema-only validation is compatible. It may inspect `df.schema`, column names, data types, and nullability metadata.
It must not trigger Spark jobs.

Compiler traceability generation is compatible when it records compile-time or generated-code metadata. Runtime traceability
hooks are out of scope and must not be introduced by streaming-compatible generated code.

Watermarks are compatible when declared with `watermark(field, delay=...)` before the stateful operation they support.
Batch grouped aggregations are fully supported. Streaming business-key aggregations follow PySpark semantics: they are
compatible with caller-owned `update` or `complete` output mode, but retain unbounded state because a watermark alone
does not identify rows that can be removed. Event-time and session-window aggregations require a compiler-visible
watermark on the grouped event-time field. Bounded stream-stream rowset joins are compatible when both inputs are
declared `streaming=True`, both joined frames have watermarks, and the predicate includes
`event_time_between(left_time, right_time, upper=...)`.

## Deferred or Rejected Operations

These operations are not streaming-compatible:

- global `orderBy(...)` or `sort(...)` on the streaming current DataFrame;
- `limit(...)`, `offset(...)`, or global top-N operations;
- `distinct(...)` or `dropDuplicates(...)`, including Structure `distinct(...)` and `drop_duplicates(...)` without a
  preceding watermark;
- event-time/window aggregations whose grouped event-time field is not preceded by a matching watermark;
- chained windowed/stateful aggregations beyond the admitted single-stage event-time and session-window shapes;
- ranking or analytic window functions, including Structure `row_number(...)`, `rank(...)`, `dense_rank(...)`,
  `lag(...)`, `lead(...)`, `rolling_sum(...)`, `rolling_avg(...)`, `rolling_min(...)`, and `rolling_max(...)`;
- selected-row helpers, including Structure `latest_by(...)`, `earliest_by(...)`, `dedupe_latest_by(...)`, and
  `dedupe_earliest_by(...)`;
- stream-stream joins that lack declared streaming input modes, watermarks, or event-time bounds;
- cross or anti joins involving the streaming current DataFrame;
- Pandas UDFs, RDD operations, `mapInPandas`, and `foreachPartition`;
- local Spark actions such as `collect()`, `count()`, `toPandas()`, `show()`, and `take()`;
- arbitrary hooks unless marked streaming-safe.

Some of these operations are supported by Spark Structured Streaming under specific watermarks, output modes, or state
policies. Structure admits only the shapes whose transformation policy is compiler-visible.

## Joins

Structure allows stream-static joins when the current pipeline DataFrame may be streaming and the joined input is
static. It also admits bounded stream-stream joins when both inputs declare streaming mode, both sides have watermarks,
and the predicate includes a compiler-visible `event_time_between(...)` bound.

Accepted:

```python
lookup_join(
    on=order.customer_id == customer.id,
    how="left",
    hint="broadcast",
)
```

Rules:

- `"left"` and `"inner"` are allowed for stream-static joins.
- `exists(...)` is allowed as stream-static left-semi filtering.
- The current pipeline side may be streaming.
- The joined input side is static unless the input declaration says `streaming=True`.
- Join conditions must satisfy `JoinSemantics.spec.md`.
- `lookup_join(...)` uniqueness warnings still apply; streaming compatibility does not prove uniqueness.
- `"broadcast"` is compatible only for the static joined side.

Rejected:

- stream-stream joins without explicit streaming input modes, watermarks, or event-time bounds;
- cross and anti stream joins;
- unmodeled chains of stateful dedupe, aggregation, and joins;
- join hints that apply to the streaming side.

`exists(...)` and `inner_join(...)` are compatible with static side inputs in principle because they do not require
streaming state by themselves. Deduped lookup joins remain batch-only until a streaming-specific design
owns tie checking, watermark assumptions, and output-mode behavior.

The checker should make the runtime-shape assumption explicit in diagnostics. If Structure later adds input metadata,
the same rules can be applied using declared input modes instead of assumptions.

## Hooks

Hooks are opaque because Structure cannot inspect arbitrary PySpark code safely.

Default rule:

- Any `@raw` hook makes the transform streaming-unknown for v1 compatibility.

Opt-in rule:

```python
@raw(inout=lane(orders) | lane(orders), streaming=True)
def remove_negative_totals(self, *, orders, spark, ctx):
    return orders.where(F.col("total") >= 0)
```

`streaming=True` is an author promise with this meaning:

- The hook returns a DataFrame.
- The hook does not call Spark actions.
- The hook does not convert to RDD, Pandas, local Python collections, or external side effects.
- The hook does not call `readStream`, `writeStream`, `start()`, or query lifecycle APIs.
- The hook does not introduce stateful streaming operations outside this reference.
- Any extra input selected by a hook binding must be static unless a later specification declares otherwise.

The checker does not need to parse hook bodies in v1. It should validate the hook signature and record that
streaming-safe hooks are trusted boundaries in traceability and diagnostics.

## Validation

Input, intermediate, and output validation remain streaming-compatible only when validation is schema-only.

Compatible checks:

- required columns;
- unexpected columns when strict schema mode is enabled;
- Spark data types;
- nullable flags where Spark metadata is reliable;
- nested struct, array, and map shape where available from schema metadata.

Not compatible in v1:

- validation that calls `count()`, `collect()`, `head()`, or equivalent actions;
- row-level constraints that require scanning data;
- uniqueness checks that require grouping or aggregation;
- sampling-based validation.

If any enabled validation phase uses `schema_and_constraints`, the streaming compatibility checker must classify the
plan as batch-only unless every enabled constraint in that phase is proven schema-only.

## IR Contract

The streaming compatibility checker consumes TransformPlan IR after symbolic execution and before code generation.

Each IR operation should expose a streaming support classification:

```text
StreamingSupport
  compatible
  batch_only
  unknown
```

Minimum metadata:

- operation kind, such as `Project`, `Filter`, `Join`, `HookCall`, or `ValidateSchema`;
- source transform and step method;
- source location or expression text when available;
- referenced input scopes;
- whether an input is the current pipeline input or a joined side input;
- join type and hint for joins;
- validation mode for validation operations;
- `streaming` for hooks.

The checker folds operation classifications into a transform-level result:

- `compatible` when every operation is compatible;
- `batch_only` when at least one operation is known to be incompatible;
- `unknown` when at least one operation is opaque and none are known incompatible.

Unknown is acceptable for batch generation but must fail an explicit streaming-compatible requirement.

## Compile-Time Checks

The checker must run without starting Spark and without importing PySpark when possible.

Required checks:

1. Reject or warn on operations not listed as supported in this reference.
2. Reject stream-stream join shapes for explicit streaming-compatible transforms.
3. Reject global sorts, aggregations, deduplication, limits, and actions in compiled DSL operations.
4. Reject or warn on hooks without `streaming=True`.
5. Reject `streaming=True` hooks with invalid hook signatures.
6. Reject schema-and-constraints validation when constraints are not schema-only.
7. Preserve streaming compatibility status in compile reports and compiler traceability metadata.
8. Link diagnostics to this reference.

The checker should be conservative. If it cannot prove an operation is compatible, it should classify it as unknown
rather than compatible.

## Diagnostics

Example:

```text
CompileError STREAM-E0801: Transform is not streaming-compatible

Transform:
  EnrichOrders

Step method:
  add_customer

Operation:
  join customers#1

Problem:
  stream-stream joins require declared streaming input modes, watermarks on both event-time fields, and a compiler-
  visible event-time bound.

Use:
  declare both streaming inputs, add watermarks, add event_time_between(...), or keep this transform batch-only.

See docs/background/Execution.back.md
```

Hook example:

```text
CompileWarning STREAM-W0801: Hook streaming compatibility is unknown

Transform:
  NormalizeOrders

Hook:
  remove_negative_totals after normalize

Problem:
  Hooks are arbitrary PySpark code. Structure cannot prove this hook is streaming-compatible.

Use:
  mark the hook as @raw(streaming=True) only if it avoids actions, RDD/Pandas conversion,
  readStream/writeStream, and stateful streaming operations.

See docs/background/Execution.back.md
```

## Generated Code Requirements

Generated PySpark must:

- use DataFrame and Column operations only for compiled DSL operations;
- avoid Spark actions;
- avoid RDD and Pandas conversion;
- avoid `readStream`, `writeStream`, query starts, triggers, and checkpoints;
- keep schema-only validation action-free;
- call streaming-safe hooks exactly like batch hooks, without lifecycle wrapping;
- keep generated code reviewable and deterministic.

Generated PySpark may use the same code path for batch and streaming DataFrames. Separate batch and streaming generated
classes are not required in v1.
