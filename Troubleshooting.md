# Troubleshooting

## Large batch plans with small inputs

Repeated branches can grow Spark's logical plan even when the input contains only a few rows. Under
`[tool.structure.plugin.pyspark]`, `plan_boundaries="auto"` uses temporary views for shared batch frames;
`off` disables them for diagnosis and `strict` applies them after every non-final step. Streaming DataFrames bypass
these boundaries. The old `connect_plan_boundaries` key must be renamed to `plan_boundaries`.

Views do not truncate Catalyst lineage. If the driver still exhausts its heap, use an explicit checkpoint at the
reused stage and configure caller-owned checkpoint storage. Search already does this inside `FuseDocuments` for
batch runs. Close the Structure session or generated transform after consuming lazy results to release owned views.
Warnings `PYSPARK-W2701` through `PYSPARK-W2704` identify repeated reuse, costly fan-out, repeated checks, and large
output paths in the query plan. Suppress one exact warning with `[tool.structure].disable = ["PYSPARK-W2701"]`, or use
`warn_on_lineage_growth = false` to suppress the whole query plan growth family while diagnosing a build.
See [configuration](docs/Configuration.md#performance-policy), [diagnostics](docs/Diagnostics.md), and
[Search checkpoint setup](examples/search/Readme.md#batch-document-search-lineage).

Whole-relation checks such as `require_unique` compare the data with an aggregate derived from the same data. If that
data already requires a large query plan, the check copies that work into another branch. Put an explicit checkpoint
**before** the check, not only after the final result. Search does this inside `FuseDocuments` before candidate
uniqueness checks, and again before feedback reranking. Keep the checks: removing them can silently admit bad data.
Batch `param_join` uses a bounded, run-scoped reusable policy check to avoid a separate copy of the policy query plan.
For timing instructions, see [integration profiling](docs/dev/Testing.md#integration-tests).
For the broader phase-by-phase performance method and Search benchmark evidence, see
[Performance troubleshooting](docs/troubleshooting/performance/Performance.trbl.md).

## A Data Assertion Did Not Run

`require_unique`, `require_all`, `require_reference`, and `require_parent_hierarchy` build lazy Spark guards.
Calling `run()` does not by itself launch a validation job; an explicit eager checkpoint can execute guards during
the run. A consuming action evaluates guards retained in the plan; Spark can remove unused work, including checks
behind a constant-false filter or `limit(0)`.
A successful partial or empty result is not certification of the whole input.

To check a complete declared relation, consume that relation with an action such as `count()` before deriving a
partial result. This is an explicit application action and incurs Spark work. It does not create a durable audit
of a changing source. See [relation assertion semantics](docs/api/Relations.api.md#relation-assertions).

Spark Connect server stack traces normally mention RDDs even when the client uses only supported DataFrame APIs.
An assertion failing inside an eager checkpoint should report its original `REL-E0702` (duplicate keys) or other
validation error, not `CONNECT-E2601`. If an older Structure runtime mislabels that failure, update the runtime and
fix the invalid input identified by the original exception; switching backends is not the remedy.

## A Raw Hook Returned the Wrong Schema

A message naming `Hook`, a relation, and a missing or incompatible column identifies the hook return boundary.
For example, dropping `content` while returning a relation whose schema requires it fails before the next step.
Restore the column or update the declared schema and downstream use together. Metadata validation does not run
the hook during compilation or scan its returned rows. Use `SchemaMode.ALLOW_EXTRA_COLUMNS` when extra columns
are intentional, and `project_output=True` when the output should retain only its declared fields.

## String Addition Rejects Mixed Types

String `+` requires two String operands. Convert a numeric expression explicitly, for example
`"count=" + row.count.cast(types.string())`. Arrays and binary values use `concat(...)` instead.

A nullable String is accepted, but a null value makes the concatenation null. Use `coalesce(row.name, "") + "!"`
when missing names should behave as empty strings. Bare `None` has no String type; use
`literal(None).cast(types.string())` if you deliberately need a null string expression.
See [Expressions API](docs/api/Expressions.api.md#general-column-transformations).

## Disk-less Source Transform Is Unavailable or Ambiguous

When calling `session.run(transform="package.module:Transform", ...)`, Structure reports that no source transform is
compiled or that the selected transform is ambiguous.

Compile the source tree into that session first with `session.compile(sources)`. If two source trees expose the same
module and class name, give each variant a distinct Python package root. See
[disk-less source compilation](docs/dev/specifications/DisklessSourceCompilation.spec.md).

## Input DataFrame Column Is Not a Python Identifier

Use a Python-safe field name and point `alias` at the real Spark column:

```python
promotion_code = string(nullable=True, alias="promo-code")
```

Transform code uses `promotion_code`. Spark schemas, validation, expression reads, and projection output use
`promo-code`. Aliases are schema-local unless inherited. Structure passes alias strings through to Spark, so
choose Spark-compatible physical column names or normalize the DataFrame before calling Structure.

## Nested Struct Assignment Fails

For a field declared as `struct(Address, ...)`, assign either a compatible whole struct expression or construct
the nested schema explicitly:

```python
shipping=Address(
    city=trim(order.shipping.city),
    postal_code=order.shipping.postal_code,
)
```

Structure checks the nested schema identity, so another schema with the same fields is not enough. If you only need to
change one child field, construct the full nested value for now; partial nested updates are planned but not part of the
current supported surface.

## Streaming Lifecycle Code Appears in a Transform or Hook

If streaming diagnostics mention `STREAM-E0801` or `STREAM-W0801` around lifecycle code, keep `readStream`,
`writeStream`, checkpoint locations, triggers, output modes, query names, `start()`, `stop()`, `awaitTermination()`,
`foreach`, and `foreachBatch` outside Structure transforms.

Structure transforms should return DataFrame plans. Put lifecycle calls in caller-owned PySpark code such as
`examples/streams/adoption.py`, then pass the streaming DataFrame into online or generated Structure execution. See
[Streaming API](docs/api/Streaming.api.md) and
[Spark Streaming](docs/dev/specifications/SparkStreaming.spec.md).
