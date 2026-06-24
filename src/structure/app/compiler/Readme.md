# Compiler App

## Purpose
The compiler app owns the pure-Python path from authored Structure DSL classes to backend-neutral facts about a
transform. It is the compile-time brain of the system and must stay independent of live Spark, Java, clusters, or
PySpark imports.

## Dependency Exchanges
The app consumes DSL declarations, schema classes, expression objects, and hook metadata, then emits `TransformPlan`
IR, compiler diagnostics, compileability reports, and traceability metadata. Downstream target and runtime apps consume
those outputs; upstream CLI, runtime session, and tests call compiler APIs to validate source and prepare execution.

The compound `compiler` API endpoint groups command factories by compiler concern:

```python
compiler.frontend.compile()
compiler.compileability.streaming()
compiler.traceability.build()
```

Each subcommand returns a fresh action instance. IR, diagnostic, streaming, and traceability model types remain
available from the `api/` packages as simplified imports.

## Inner Workings
Compiler apps divide the pipeline by responsibility: `frontend` inspects source classes, `symbolic_execution`
captures `where(...)` and `join_one(...)` effects, `ir` stores plan records, `compileability` classifies target
fitness, `diagnostics` defines compile errors, and `traceability` maps source, IR, recipes, and generated artifacts.
