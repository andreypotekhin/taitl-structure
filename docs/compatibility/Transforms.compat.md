# Transforms Compatibility

This is the compatibility companion to the [API reference](../api/Transforms.api.md). It records Structure contracts alongside the corresponding PySpark API forms and examples for read-through. The shared baseline is the public PySpark 3.5.x/4.0.x intersection; Connect is claimed only where runtime evidence is recorded.

## Transform and extension helpers

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `Transform` | DataFrame pipeline class | `Transform` | yes | yes | Base class for a compiler-visible pipeline; subclasses declare inputs, outputs, and typed steps. |
| `input(...)` | DataFrame input | `orders = input(OrderRaw)` | yes | yes | `input(...)`, `output(...)`, and `lane(...)` declare named transform boundaries. A graph may collect explicit output sources with `outputs = output(name=stage.output, ...)` while keeping schemas declared separately. |
| `output(...)` | DataFrame result | `published = output(OrderPublished)` | yes | yes | `input(...)`, `output(...)`, and `lane(...)` declare named transform boundaries. A graph may collect explicit output sources with `outputs = output(name=stage.output, ...)` while keeping schemas declared separately. |
| `sink(WriterClass)` | Declared row-writer handoff | `publish_alerts = sink(AlertWriter)` | 3.5 only | 4.0 only | A named writer declaration binds to a typed step parameter by unique class or explicit `@step(sink=...)`. The writer must subclass `structure.plugin.pyspark.Sink`; Structure does not instantiate it. |
| `foreach(row, sink)` | `DataFrame.foreach` / `DataStreamWriter.foreach` | `foreach(alert, sink)` | 3.5 only | 4.0 only | Captures an association with the exact returned final output row. The caller receives `result.publish_alerts.dataframe` and `.writer`, then invokes native PySpark. Live evidence covers classic PySpark only; Spark Connect is unclaimed. |
| `lane(...)` | Intermediate DataFrame | `clean = lane(OrderClean)` | yes | yes | `input(...)`, `output(...)`, and `lane(...)` declare named transform boundaries. A graph may collect explicit output sources with `outputs = output(name=stage.output, ...)` while keeping schemas declared separately. |
| `stage(...)` | Explicit composed-stage compatibility API | `stage(order.value)` | yes | yes | In a class-body stage graph, `stage(...)` is optional: assigning a transform invocation directly, such as `normalized = NormalizeOrders(orders=orders)`, declares the assignment as a stage. Direct assignments can be chained with `normalized.output`; ordinary Python assignments are ignored. The explicit `stage(...)` form remains supported. |
| `@transform(...)` | Pipeline declaration | `@transform\nclass Publish(Transform): pass` | yes | yes | `@transform(...)` accepts transform-level target and streaming options. |
| `project(...)` | `select` | `return project(order, OrderPublished)(id=order.id)` | yes | yes | `project(...)` builds typed projections; schema constructors and `Schema.project(...)` are often shorter. |
| `where(...)` | `filter`, `where` | `where(order.total > 0)` | yes | yes | `where(...)` accepts symbolic Boolean expressions and can be chained with `.where(...)`. |
| `@step(...)` | `persist` | `@step(cache=True)` | yes | yes | Undecorated public methods with schema input/return annotations are also steps; `@step(...)` disambiguates bindings. |
| `@step(...)` | `persist` | `@step(cache=StorageLevel.MEMORY_AND_DISK)` | yes | yes | Undecorated public methods with schema input/return annotations are also steps; `@step(...)` disambiguates bindings. |
| `@special(...)` | Optional expression metadata or named helper rendering | `@special(type="expr")\ndef clean(v): return trim(v)` | yes | yes | `@special(type="ignore")` excludes code from compiler-visible logic; `@special(type="opaque")` marks runtime code whose body Structure does not inspect. Direct calls from compiled code fail. `Sink` subclasses receive the same call guard through inheritance. |
| `Compiler.frontend.analyze(...)` | Structural transform plan | `plan = Compiler.frontend.analyze()(Publish)` | yes | yes | `Compiler.frontend.analyze()` does not invoke step methods or start a Spark job. |
| `Compiler.frontend.compile(...)` | Selected-platform compilation | `compiled = Compiler.frontend.compile()(Publish)` | yes | yes | `Compiler.frontend.compile()` authors and compiles for the selected platform but does not start a Spark job. |
| `StructureCompileError` | Compile-time diagnostic | `StructureCompileError(order.value)` | yes | yes | `StructureCompileError` exposes a rendered diagnostic with remediation. See the [Transforms background](../background/Transform.back.md) and [Hooks reference](../background/HookSemantics.back.md). |

## Transform and hook boundaries

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `@raw(...)` | Caller-owned DataFrame code | `@raw(inout=lane(clean) \| output(published))` | yes | yes | `@raw(...)` is the explicit opaque boundary: Structure validates its binding declaration, not the hook body. |
| `SchemaMode` | Hook schema policy | `@raw(..., schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS)` | — | — | `STRICT` is the default and requires the declared output schema; `ALLOW_EXTRA_COLUMNS` permits additional columns at that hook boundary. |
## Unsupported

These PySpark functions or behaviors have no equivalent admitted Structure contract. Use the stated caller-owned or typed alternative.

| PySpark parity | Details |
| --- | --- |
| `call_udf` | Status: `caller-owned-guided`. Registered-function lookup depends on session state and an external result contract. Migration: Register and invoke through native PySpark at a boundary with an explicitly declared output schema. |
| `pandas_udf` | Status: `caller-owned-guided`. Python callback execution, Arrow behavior, and result typing are runtime-owned. Migration: Use native PySpark with an explicit return type and keep the callback at a declared boundary. |
| `udf` | Status: `caller-owned-guided`. Python callback execution and Arrow selection do not have a compiler-visible contract. Migration: Use native PySpark with an explicit return type at a declared boundary. |
| `udtf` | Status: `caller-owned-guided`. Callback-defined row expansion and schema depend on runtime class behavior; the optional return-type signature varies by target. Migration: Use native PySpark with an explicitly declared output schema and cardinality boundary. |
