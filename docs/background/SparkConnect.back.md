# Spark Connect

Spark Connect is a PySpark target variant. It lets Structure run completed compiler-visible batch transforms with a
caller-supplied Spark Connect session while keeping the same Structure DSL and generated-code API. The governing
[Spark Connect specification](../dev/specifications/SparkConnect.spec.md) and [design](../dev/design/SparkConnect.design.md) define
its configuration, runtime boundaries, and support contract.

## Reader Flow

Spark Connect support has four separate questions:

```text
configuration -> select the spark-connect variant
compiler      -> compile the same Spark-free Structure plan
runtime       -> execute through a caller-owned remote session
verification  -> prove parity without classic-only internals
```

The variant changes the target capability profile and runtime boundary; it does not create a second Structure DSL.
Start with configuration, inspect the supported feature families, then check hook and runtime ownership before moving a
project from ordinary PySpark to Spark Connect.

## Configuration

```toml
[tool.structure]
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "spark-connect"

[tool.structure.plugin.pyspark]
variant = "spark-connect"
connect_plan_boundaries = "auto"
```

Ordinary PySpark remains the default:

```toml
target_variant = "ordinary"
```

Configuration is resolved before compilation. A project can therefore run `structure check` and `structure compile`
without connecting to Spark, while execution and generated-code integration tests select a real Connect session. The
resolved variant becomes part of capability decisions and generated artifact identity.

## What Is Supported

Spark Connect support covers completed batch features that lower through public PySpark DataFrame and Column APIs:

- projections, filters, casts, literals, and expression helpers;
- lookup joins, completed analytical joins, and implemented rowset joins such as right, full, explicit cross, non-equi,
  and
  disjunctive joins;
- first-slice aggregations plus implemented advanced analytical helpers such as rollup, cube, grouping metadata,
  additional aggregate metrics, metric-local filters, reusable windows, distribution/value/window aggregate helpers,
  selected-row helpers, ranking, lag/lead, rolling metrics, and dedupe helpers;
- the implemented compiler-visible array and map helper set;
- bounded ordered `scan(...)` recurrences, lowered through public higher-order DataFrame/Column functions;
- explicit scalar Python UDFs through Spark Connect's public UDF API, with the same generated/online ownership rules;
- batch-only `exactly_one(...)` relation assertions implemented with public aggregate and join expressions;
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
- hidden fallback to Python UDFs, local collection, row-wise loops, or SQL string rewrites. Explicit scalar UDFs are
  supported only when declared with `@special(type="udf")`.
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

The metadata distinction is important: schema generation may need a remote metadata request, while compiler checks must
remain Spark-free. A tool may ask the caller's Connect session for supported metadata, but it must not silently fall
back to private classic fields or infer a schema by collecting user data.

## Runtime Use

Create the Spark Connect session outside Structure and pass it in:

```python
session = StructureSession(spark=spark, ctx=ctx, config=config)
result = NormalizeOrders(orders=orders_df).run(session)
```

Generated classes keep the same shape and expose `close()` for Structure-created temporary-view resources:

```python
generated = NormalizeOrdersGenerated(spark=spark, ctx=ctx)
result = generated.run(orders=orders_df)
# generated.close() after lazy results have been materialized or released
```

Structure does not create or manage the remote Spark Connect server.

The caller owns session creation, authentication, remote endpoint lifecycle, input DataFrames, actions, result
materialization, and cleanup. Structure owns only transform invocation and the semantic plan it supplies to the target.
This ownership is the same as ordinary execution except that the Connect variant rejects classic-only runtime access.

For Connect, input and final-output schema checks remain strict. Intermediate checks are disabled by default because
`DataFrame.schema` is a remote analysis request; set `validate_intermediate = true` for exhaustive diagnostics. The
`connect_plan_boundaries` option independently limits serialized logical-plan growth with session-scoped temporary
views. `StructureSession.close()` drops only Structure-created views and never stops the caller's Spark session.

For a migration, keep the transform invocation unchanged and isolate target-specific code at the hook or session edge:

```python
config = StructureConfig.resolve(
    project_root=project_root,
    overrides={"target_variant": "spark-connect"},
)
session = StructureSession(spark=connect_spark, ctx=ctx, config=config)
result = NormalizeOrders(orders=orders_df).run(session)
```

Compiler-visible expressions, schemas, joins, aggregates, and validation should not need a Connect-specific branch.
Only code that intentionally touches the runtime or a raw hook should require a target decision.

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

Support is feature-family based, not version-name based. A profile may claim Connect support for expressions and joins
while rejecting a newly introduced hook or relation operation until that feature has public-API lowering and parity
evidence. A successful import is not evidence of runtime support.

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

The smallest useful verification matrix is:

| Path | Ordinary PySpark | Spark Connect |
| --- | --- | --- |
| Spark-free `check` | accepted | accepted |
| Spark-free `compile` | target recipes render | Connect recipes render |
| online execution | live session parity | live remote session parity |
| generated execution | result and schema parity | result and schema parity |
| prohibited runtime access | ordinary-only behavior may run | `CONNECT-E2601` |

For each admitted feature, compare output names, schema, rows, null behavior, diagnostics, and generated-source scans.
Run an ordered comparison only when the transform explicitly promises order. When a Connect limitation is discovered,
record it as a capability decision and diagnostic rather than adding a hidden fallback.

StructureTools may use Connect-supported metadata paths for schema generation. Unsupported path or table metadata access
must preserve the cause, name Spark Connect, and suggest an explicit schema.

## Diagnostics And Repair Order

When a Connect execution fails, diagnose from the outside in:

1. Confirm that `target_variant` resolves to `spark-connect` and that the caller supplied a Connect session.
2. Check the capability decision for the operation family named in the failure.
3. Search hooks and generated code for classic-only access such as `sparkContext`, `rdd`, `_jvm`, `_jdf`, or Py4J.
4. Replace the operation with compiler-visible DSL or public Connect APIs, or explicitly select ordinary PySpark.
5. Re-run generated and online parity against the same input fixtures.

The repair must not be “catch the Connect error and execute locally.” That changes the target boundary and can
silently move data or semantics outside the caller's remote session.

## Acceptance Contract

Spark Connect support is complete for a feature when configuration resolution, Spark-free compilation, capability
admission, online execution, generated execution, public-API source scans, hook target checks, metadata diagnostics,
and parity evidence agree. Streaming ownership, storage writes, and unsupported classic internals remain explicit
boundaries until their own contracts are admitted.
