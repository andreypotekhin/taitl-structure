# Runtime Schemas App

## Purpose
The runtime schemas app builds runtime schema materializations for a lowered transform. It gives session callers a
consistent view of input, intermediate, and output schemas without coupling them to the PySpark target internals.

## Dependency Exchanges
The app consumes `PySparkExecutionPlan` schema references and asks the PySpark target to materialize each Structure
schema into the runtime representation. It returns `TransformSchemas`, a compact mapping of input, step, and output
schema names to materialized schema objects.

## Inner Workings
`BuildTransformSchemas` traverses inputs, step outputs, and final outputs, deduplicates by schema name, and freezes the
result into `TransformSchemas`. The app is deliberately narrow so future targets can replace materialization without
changing session behavior.
