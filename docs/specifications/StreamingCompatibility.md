# Streaming Compatibility

## Purpose

Structure v1 generates PySpark DataFrame transforms. It does not generate Spark Structured Streaming jobs. A generated
transform is streaming-compatible when a caller can pass a streaming DataFrame as the current pipeline input and Spark
can analyze the resulting DataFrame plan without Structure adding unsupported streaming operations, actions, stateful
streaming features, or streaming lifecycle code.

The v1 contract is intentionally narrow: row-local projection, row-local filtering, schema-only validation, and
stream-static lookup joins are in scope. Watermarks, output modes, triggers, checkpoints, streaming sources, streaming
sinks, stream-stream joins, and stateful aggregations are outside v1.

## Definition

Streaming compatibility means all of these are true:

- Generated code accepts ordinary PySpark `DataFrame` objects and does not require a batch-only DataFrame.
- The current pipeline DataFrame may be streaming.
- Side input DataFrames used for lookup joins are treated as static batch DataFrames.
- Generated operations are supported by Spark Structured Streaming for that runtime shape.
- Generated code does not call Spark actions such as `collect()`, `count()`, `toPandas()`, or `show()`.
- Generated code does not create `readStream`, call `writeStream`, set triggers, set checkpoints, or start queries.
- Opaque hooks are absent or explicitly marked streaming-safe.

Streaming compatibility does not mean Structure guarantees every Spark expression is valid for every Spark version or
every output mode. Structure checks the v1 operation contract at compile time and leaves query lifecycle choices to the
caller.

## Runtime Shape

The v1 streaming-compatible runtime shape is one streaming current pipeline DataFrame plus zero or more static side
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

- The current pipeline DataFrame is the DataFrame flowing through the source-ordered subtransform chain.
- Additional named inputs referenced only through joins are static side inputs in v1.
- Passing a streaming DataFrame as a joined side input creates a stream-stream join and is outside v1.
- Transforms with two independent streaming roots are outside v1 because Structure does not model streaming
  synchronization, watermarks, or output modes.
- Generated code should not branch on `df.isStreaming`; the same transform body should work for batch and streaming
  inputs when the operation contract is satisfied.

## Configuration

The seed configuration includes:

```toml
streaming_compatibility_checks = true
```

When `streaming_compatibility_checks = true`, `structure check` and `structure compile` run a streaming compatibility
pass over the TransformPlan IR.

Transform-level opt-in uses:

```python
@transform(streaming_compatible=True)
class EnrichOrders(Transform):
    enriched = output(OrderEnriched)
    ...
```

Severity rules:

- If `streaming_compatibility_checks = false`, Structure emits no streaming compatibility diagnostics.
- If checks are enabled and a transform does not opt in with `streaming_compatible=True`, incompatible operations emit
  warnings.
- If checks are enabled and a transform opts in with `streaming_compatible=True`, incompatible or unknown operations
  emit errors.
- If checks are disabled and a transform opts in with `streaming_compatible=True`, the transform-level marker wins and
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

Expression-based derived columns are compatible when they lower to Spark SQL functions or Column operators that do not
require cross-row state, local collection, Python UDF execution, or RDD conversion.

Schema-only validation is compatible. It may inspect `df.schema`, column names, data types, and nullability metadata.
It must not trigger Spark jobs.

Compiler traceability generation is compatible when it records compile-time or generated-code metadata. Runtime traceability
hooks are out of scope for v1 and must not be introduced by streaming-compatible generated code.

## Deferred or Rejected Operations

These operations are not streaming-compatible in v1:

- global `orderBy(...)` or `sort(...)` on the streaming current DataFrame;
- `limit(...)`, `offset(...)`, or global top-N operations;
- `distinct(...)` or `dropDuplicates(...)`;
- aggregations, including `groupBy(...).agg(...)`;
- windowed aggregations;
- ranking or analytic window functions;
- stream-stream joins;
- right, full, cross, semi, or anti joins involving the streaming current DataFrame;
- Python UDFs, Pandas UDFs, RDD operations, `mapInPandas`, and `foreachPartition`;
- local Spark actions such as `collect()`, `count()`, `toPandas()`, `show()`, and `take()`;
- arbitrary hooks unless marked streaming-safe.

Some of these operations are supported by Spark Structured Streaming under specific watermarks, output modes, or state
policies. Structure defers them because v1 does not model those lifecycle and state contracts.

## Joins

Structure v1 allows stream-static joins only when the current pipeline DataFrame may be streaming and the joined input
is static.

Accepted:

```python
customer = self.customers.join_one(
    on=self.customers.id == order.customer_id,
    how=Join.LEFT,
    hint=JoinHint.BROADCAST,
)
```

Rules:

- `Join.LEFT` and `Join.INNER` are allowed for stream-static joins.
- The current pipeline side may be streaming.
- The joined input side must be static.
- Join conditions must satisfy `JoinSemantics.spec.md`.
- `join_one(...)` uniqueness warnings still apply; streaming compatibility does not prove uniqueness.
- `JoinHint.BROADCAST` is compatible only for the static joined side.
- A side input that may be streaming must be rejected for v1 streaming compatibility.

Rejected in v1:

- stream-stream joins;
- row-multiplying joins such as `join_many(...)` until v2 implements them;
- joins that require watermarks;
- outer stream-stream joins;
- stateful deduplication before or after a join;
- join hints that apply to the streaming side.

`join_many(...)` (v2) is compatible with static side inputs in principle because row multiplication is intentional and
does not require streaming state by itself. It remains outside v1 only because the row-multiplying DSL is staged for
v2.

The checker should make the runtime-shape assumption explicit in diagnostics. If Structure later adds input metadata,
the same rules can be applied using declared input modes instead of assumptions.

## Hooks

Hooks are opaque because Structure cannot inspect arbitrary PySpark code safely.

Default rule:

- Any `@before(...)` or `@after(...)` hook makes the transform streaming-unknown for v1 compatibility.

Opt-in rule:

```python
@after(normalize, streaming_safe=True)
def remove_negative_totals(self, *, df, spark, ctx):
    return df.where(F.col("total") >= 0)
```

`streaming_safe=True` is an author promise with this meaning:

- The hook returns a DataFrame.
- The hook does not call Spark actions.
- The hook does not convert to RDD, Pandas, local Python collections, or external side effects.
- The hook does not call `readStream`, `writeStream`, `start()`, or query lifecycle APIs.
- The hook does not introduce stateful streaming operations outside this specification.
- If `pass_inputs=True`, any joined or consulted input DataFrames are static unless a later spec declares otherwise.

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
- source transform and subtransform;
- source location or expression text when available;
- referenced input scopes;
- whether an input is the current pipeline input or a joined side input;
- join type and hint for joins;
- validation mode for validation operations;
- `streaming_safe` for hooks.

The checker folds operation classifications into a transform-level result:

- `compatible` when every operation is compatible;
- `batch_only` when at least one operation is known to be incompatible;
- `unknown` when at least one operation is opaque and none are known incompatible.

Unknown is acceptable for batch generation but must fail an explicit streaming-compatible requirement.

## Compile-Time Checks

The checker must run without starting Spark and without importing PySpark when possible.

Required checks:

1. Reject or warn on operations not listed as supported in this specification.
2. Reject stream-stream join shapes for explicit streaming-compatible transforms.
3. Reject global sorts, aggregations, deduplication, limits, and actions in compiled DSL operations.
4. Reject or warn on hooks without `streaming_safe=True`.
5. Reject `streaming_safe=True` hooks with invalid hook signatures.
6. Reject schema-and-constraints validation when constraints are not schema-only.
7. Preserve streaming compatibility status in compile reports and compiler traceability metadata.
8. Link diagnostics to this specification.

The checker should be conservative. If it cannot prove an operation is compatible, it should classify it as unknown
rather than compatible.

## Diagnostics

Diagnostics should include:

- error or warning code;
- transform class;
- subtransform;
- operation kind;
- source expression or hook name;
- compatibility classification;
- problem;
- suggested fix;
- link to this specification.

Example:

```text
CompileError STREAM-E0801: Transform is not streaming-compatible

Transform:
  EnrichOrders

Subtransform:
  add_customer

Operation:
  join customers#1

Problem:
  v1 streaming compatibility supports stream-static joins only. The joined input may be streaming, which would create
  a stream-stream join.

Use:
  pass a static lookup DataFrame for customers, or keep this transform batch-only.

See docs/specifications/StreamingCompatibility.md
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
  mark the hook as @after(normalize, streaming_safe=True) only if it avoids actions, RDD/Pandas conversion,
  readStream/writeStream, and stateful streaming operations.

See docs/specifications/StreamingCompatibility.md
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

## Acceptance Criteria

The implementation is complete when tests prove these scenarios:

- A projection-only transform is classified streaming-compatible.
- A transform with row-local `where(...)` filters is classified streaming-compatible.
- A transform using `lower`, `trim`, `coalesce`, and explicit parsing helpers in projections is compatible.
- Schema-only input, intermediate, and output validation are compatible and do not call Spark actions.
- Any validation phase using `schema_and_constraints` is batch-only when enabled constraints are not schema-only.
- A stream-static `Join.LEFT` lookup join is compatible when the joined side is static.
- A stream-static `Join.INNER` lookup join is compatible when the joined side is static.
- `join_one(...)` uniqueness warnings still appear independently from streaming compatibility.
- A possible stream-stream join is rejected for explicit streaming-compatible transforms.
- A hook without `streaming_safe=True` makes compatibility unknown or emits a warning.
- A hook with `streaming_safe=True` is accepted as a trusted boundary after signature validation.
- Global sort, aggregation, deduplication, limit, Python UDF, Pandas UDF, RDD conversion, and local actions are
  rejected.
- Generated code for a compatible transform contains no `readStream`, `writeStream`, `collect`, `count`, or `toPandas`.
- Diagnostics link to `docs/specifications/StreamingCompatibility.md`.
