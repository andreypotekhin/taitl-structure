# Design: Online Execution Runtime

## Purpose

The online execution runtime runs Structure transforms directly from source metadata and compiler IR. It lets v1 users
execute transforms without committing generated PySpark code while preserving the strict Spark-plan-visible behavior
that generated PySpark exists to expose.

The runtime should be thin. It coordinates Spark, configuration, runner selection, and hook context, then delegates
actual execution to focused runner components.

## Public Shape

```python
from structure import StructureSession

session = StructureSession(spark=spark, ctx=ctx)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched = result.enriched
```

`EnrichOrders(...)` creates a deferred transform invocation. `.run(session)` delegates to `session.run(transform)`.

## Components

```text
StructureSession
  spark
  ctx
  config
  plan_cache
  registry
    -> OnlinePySparkRunner
    -> GeneratedPySparkRunner
```

`StructureSession` owns runtime context and delegates. It should not grow into a compiler, Spark lifecycle manager, or
orchestrator.

`RuntimeTargetRegistry` maps `(execution_mode, target_backend)` to a runner:

```text
("online", "pyspark")    -> OnlinePySparkRunner
("generated", "pyspark") -> GeneratedPySparkRunner
```

`OnlinePySparkRunner` consumes shared PySpark execution recipes and live PySpark objects.

`GeneratedPySparkRunner` imports generated PySpark modules and calls their generated classes.

## Data Flow

```text
application creates SparkSession
  -> application creates StructureSession
  -> application constructs transform invocation with DataFrames
  -> Transform.run(session)
  -> StructureSession resolves config and runner
  -> runner compiles or imports runtime target
  -> runner validates inputs
  -> runner executes compiled operations and hooks
  -> runner validates output
  -> application receives DataFrame
```

## Online PySpark Runner

The online runner compiles the transform class to IR on demand through the same frontend used by `structure check` and
`structure compile`. It then asks the PySpark target layer to lower checked IR into shared execution recipes and
interprets those recipes as live PySpark DataFrame and Column calls.

The runner must not:

- write generated files;
- execute generated Python source text;
- create, stop, or reconfigure Spark sessions;
- call Spark actions except where an explicit validation mode later requires them;
- hide unsupported Python logic behind UDFs, RDD operations, Pandas conversion, or row-wise callbacks.

The runner must preserve generated semantics:

- input validation;
- hook input namespace creation only for `pass_inputs=True`;
- before-hook execution;
- filter, join, projection, and expression lowering;
- after-hook execution;
- intermediate and final validation;
- hook `schema_mode` and `project_output`;
- streaming compatibility rules.

## Generated PySpark Runner

The generated runner supports projects that set:

```toml
execution_mode = "generated"
```

It computes the generated module and class for the transform invocation, imports the generated class, constructs it with
`spark=session.spark` and `ctx=session.ctx`, then calls `run(**inputs)`.

Missing generated code is a runtime configuration problem, not a reason to fall back silently to online execution. The
diagnostic must tell the user to run `structure compile`, fix import roots, or change `execution_mode`.

## Shared Semantic Contract

Online execution and generated code must not become independent semantic implementations. The generated emitter owns
text concerns such as imports, formatting, and stable source output. The shared contract in
`docs/specifications/ExecutionSemanticContract.md` owns semantic concerns:

- expression function mapping;
- literal typing;
- join aliasing;
- broadcast hints;
- projection order;
- schema validation placement;
- hook ordering;
- `HookInputs` shape;
- forbidden operation guardrails.

If a shared lowering API is too large for the first vertical slice, add parity tests immediately. Shared PySpark
execution recipes must be extracted before adding joins or hooks.

## Plan Cache

`StructureSession` may cache compiled transform plans. A safe cache key includes:

- transform class identity;
- source fingerprint when available;
- resolved config fingerprint;
- Structure version;
- execution mode;
- target backend;
- target PySpark range.

The first implementation may compile on every run if the cache key is not yet reliable. The public API must leave room
for caching without changing user code.

## Error Boundaries

Runtime compilation errors should reuse compiler diagnostics. Runtime execution errors should add runtime context:

- transform class;
- execution mode;
- target backend;
- input name, hook, subtransform, or output field when known;
- problem;
- suggested fix;
- documentation link.

Generated-mode import errors must be explicit. Online mode must not silently switch to generated mode, and generated
mode must not silently switch to online mode.

## Non-Goals

The online runtime does not:

- generate streaming orchestration;
- start or stop Spark;
- manage Airflow, jobs, schedules, triggers, checkpoints, or sinks;
- provide Spark Connect support in v1;
- make hooks inspectable;
- weaken compiler performance guardrails.

## Acceptance

Online execution is implemented when:

- `StructureSession` is public;
- transform invocations bind named inputs and can be run later;
- `execution_mode = "online"` is default;
- generated mode remains available;
- online and generated paths are tested for semantic parity;
- compiler commands remain Spark-free.
