# PySpark Plugin App

## Purpose
PySpark plugin app lowers Structure IR into PySpark-specific execution recipes and renders optional generated
PySpark sources. This app is responsible for understanding PySpark schema syntax, DataFrame operation rendering,
generated runtime helpers, and generated file ownership.

## Dependency Exchanges
This app consumes 
- compiler `TransformPlan` IR, 
- DSL schemas and expressions, 
- target capabilities, 
- generated output paths. 

It returns 
- `PySparkExecutionPlan` recipe graphs,
- schema objects,
- source strings,
- generated project file maps,
- `GeneratedFileSetResult` diffs or writes, 
- traceability files used by CLI and runtime apps.

The app's main `PySpark` endpoint exposes children apps and their subcommands:
```python
PySpark.compiler.lower()
PySpark.schema.materialize()
PySpark.render.project()
PySpark.execution.online()
PySpark.files.write()
```

Internally, Compiler, Schema, Render, Execution, Files, and Capabilities are peer apps. They reach each other
only through that app's endpoint; recipe and file-result types remain public contracts of their owning app.

## Inner Workings
- PySpark.compiler.lower()'s `LowerPySparkPlan` converts IR to recipe records such as `PySparkStepRecipe`, `PySparkJoinRecipe`, and
`PySparkExpressionRecipe`; 
- Renderer actions turn those recipes into schema modules, transform classes, runtime support, and project file maps; 
- `CompareGeneratedFiles` and `WriteGeneratedFiles` handle filesystem results without changing the lowering model.
