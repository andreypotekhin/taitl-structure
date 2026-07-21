# PySpark Plugin Facade

## Purpose
`PySpark` is the bundled plugin's composition root. It exposes the plugin's long-lived app endpoints and adapts them
to the negotiated Plugin API v1 facets.

## Dependency Exchanges
Core reaches the plugin through negotiated schema, compiler, authoring, execution, generation, explain, and analysis
facets. PySpark peer apps reach one another only through `PySpark.compiler`, `PySpark.schema`, `PySpark.render`,
`PySpark.execution`, `PySpark.files`, `PySpark.capabilities`, and `PySpark.symbolic_execution`.

## Inner Workings
The façade owns one endpoint instance per PySpark app. Its facet adapters translate the versioned plugin contract to
those endpoint commands without exposing another app's private commands or logic.
