# Design: Execution and Data Flows

## Compile Flow

```text
1. Resolve Core configuration and transform target.
2. Discover eligible plugin metadata, then load and negotiate the selected Plugin API.
3. Discover schemas and transforms.
4. Core analyzes inheritance, bindings, lanes, source order, outputs, hooks, and routing.
5. Core invokes the plugin authoring facet for each step and stores its opaque body.
6. The plugin schema and compiler facets validate target semantics and lower the completed plan.
7. Core wraps the opaque payload in a versioned artifact envelope.
8. Core optionally asks the plugin generator for source, validates every relative path, and writes files.
```

The compiler never imports a target implementation merely to inspect configuration. For PySpark, ordinary compiler
commands need neither PySpark nor a Spark session.

## Runtime Flow

```text
1. Caller creates and owns the target runtime (a SparkSession for PySpark).
2. Caller creates StructureSession(runtime=..., target=..., context=..., config=...).
3. Caller creates a transform invocation with named declared inputs.
4. Transform.run(session) asks Core to resolve the transform's one target.
5. Core retrieves or builds a compatible artifact and calls the selected plugin executor.
6. The executor evaluates the opaque target payload and returns target output values.
7. Core exposes those values through the standard TransformResult boundary.
```

The explicit `target=` is session- or invocation-local. It does not mutate project configuration or create a process-wide
active plugin. A transform decorator target and an explicit target must agree.

## PySpark Runtime Flow

```text
caller-owned SparkSession + DataFrames
        |
        v
StructureSession(runtime=spark)
        |
        v
selected PySpark executor
        |
        v
PySpark-owned recipes -> DataFrame operations and hooks -> TransformResult
```

The PySpark executor validates inputs and outputs and applies the configured intermediate-validation policy. The
generated PySpark module renders the same lowered recipes. The caller continues to own reads, writes, Spark
configuration, streaming query construction, triggers, checkpoints, sinks, and session lifecycle.
