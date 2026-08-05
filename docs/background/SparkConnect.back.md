# Spark Connect

Spark Connect is a PySpark target variant. It lets Structure run completed compiler-visible batch transforms with a
caller-supplied Spark Connect session while keeping the same Structure DSL and generated-code API.

## Configuration

```toml
[tool.structure]
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "spark-connect"
```

Ordinary PySpark remains the default:

```toml
target_variant = "ordinary"
```

## What Is Supported

Spark Connect support covers completed v1/v2 batch features that lower through public PySpark DataFrame and Column APIs:

- projections, filters, casts, literals, and expression helpers;
- v1 joins, completed analytical joins, and implemented rowset joins such as right, full, explicit cross, non-equi, and
  disjunctive joins;
- first-slice aggregations plus implemented advanced analytical helpers such as rollup, cube, grouping metadata,
  additional aggregate metrics, metric-local filters, reusable windows, distribution/value/window aggregate helpers,
  selected-row helpers, ranking, lag/lead, rolling metrics, and dedupe helpers;
- the implemented compiler-visible array and map helper set;
- schema-only validation and strict projection;
- execution through `StructureSession`;
- generated PySpark execution with the same constructor and `run(...)` signature as ordinary PySpark.

## What Is Not Included

Spark Connect support does not include:

- Structure-owned streaming sources, sinks, triggers, watermarks, output modes, or state policies;
- storage write orchestration;
- RDD access;
- SparkContext access;
- direct JVM/Py4J access;
- `_jdf` or private classic PySpark fields;
- hidden fallback to Python UDFs, local collection, row-wise loops, or SQL string rewrites.
- deferred batch features such as same-name join-key shorthand until their owning specifications admit them.

Hooks remain user-owned PySpark code. Structure validates hook signatures and target scope, but arbitrary hook bodies
are
opaque. For Spark Connect, hook bodies must use public Connect-compatible PySpark APIs.

## Runtime Boundaries

If execution or generated-code execution detects that runtime code touched classic-only Spark internals while
`target_variant = "spark-connect"` is active, Structure raises `CONNECT-E2601`. This diagnostic covers detected
SparkContext, RDD, JVM, Py4J, and private classic-field access from hook bodies or generated transform execution.

The fix is to rewrite the code with public Spark Connect DataFrame APIs, move the logic into compiler-visible Structure
DSL, or run the job with `target_variant = "ordinary"` when the code intentionally depends on classic PySpark internals.

StructureTools schema generation can use Spark Connect metadata paths that the remote session supports. If table or
path schema extraction fails through Spark Connect, the tool names Spark Connect in the error and suggests passing an
explicit `schema=...` object.

## Runtime Use

Create the Spark Connect session outside Structure and pass it in:

```python
session = StructureSession(spark=spark, ctx=ctx, config=config)
result = NormalizeOrders(orders=orders_df).run(session)
```

Generated classes keep the same shape:

```python
result = NormalizeOrdersGenerated(spark=spark, ctx=ctx).run(orders=orders_df)
```

Structure does not create or manage the remote Spark Connect server.

## Diagnostics

When a transform asks Spark Connect to run classic-only behavior, Structure should fail before execution or generation
with a backend capability diagnostic. The diagnostic should name the unsupported feature and suggest one of these fixes:

- use `target_variant = "ordinary"` when the project depends on classic-only PySpark;
- remove SparkContext, RDD, JVM, Py4J, `_jdf`, or private-field usage;
- move hook logic into compiler-visible Structure DSL;
- rewrite hook logic with public Connect-compatible PySpark APIs.

## Support Boundary

Spark Connect is supported for completed compiler-visible batch features only after the project has parity evidence
against a real Spark Connect session. Streaming orchestration remains separate roadmap work.

## Capability And Verification Contract

Spark Connect is admitted through the PySpark capability profile, not a generic fallback. The profile must explicitly
cover the completed compiler-visible feature families it claims: expressions, joins, analytical operations, aggregates,
windows, dedupe, higher-order arrays and maps, schema-only validation, strict projection, and generated imports.
Deferred features remain unsupported decisions rather than silent rewrites.

The minimum evidence matrix covers ordinary and Spark Connect variants, PySpark 3.5.x and 4.0.x, and both online and
generated execution. If CI cannot run the full matrix, a documented manual verification script is a release blocker
before publishing a stable support claim. Evidence must include live execution/generated parity, compiler commands that
remain Spark-free, generated-source scans for `_jdf`, `sparkContext`, `.rdd`, `_jvm`, and Py4J, and clear diagnostics
for
ordinary-only hooks and runtime boundaries.

StructureTools may use Connect-supported metadata paths for schema generation. Unsupported path or table metadata access
must preserve the cause, name Spark Connect, and suggest an explicit schema.
