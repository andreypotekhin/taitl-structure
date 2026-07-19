# PySpark Platform Facade

`PySpark` is the bundled plugin's composition root. It owns one endpoint instance for each PySpark app and is the
only cross-app endpoint surface: `PySpark.compiler`, `PySpark.schema`, `PySpark.render`, `PySpark.execution`,
`PySpark.files`, `PySpark.capabilities`, and `PySpark.symbolic_execution`.

The remaining classes adapt those endpoints to the negotiated Platform API v1 facets.
