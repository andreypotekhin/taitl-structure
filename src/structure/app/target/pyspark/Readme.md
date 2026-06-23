# PySpark Target App

## Purpose
The PySpark target app lowers Structure IR into PySpark-specific execution recipes and renders optional generated
PySpark source. It is the only app that should understand PySpark schema syntax, DataFrame operation rendering,
generated runtime helpers, and generated file ownership.

## Dependency Exchanges
The app consumes compiler `TransformPlan` IR, DSL schemas and expressions, target capabilities, and generated output
paths. It returns `PySparkExecutionPlan` recipe graphs, schema objects, source strings, generated project file maps,
`GeneratedFileSetResult` diffs or writes, and traceability files used by CLI and runtime apps.

The compound `pyspark` API endpoint groups commands by purpose:

```python
pyspark.plan.lower()
pyspark.schema.materialize()
pyspark.render.project()
pyspark.files.write()
```

Each subcommand returns a fresh action instance.

## Inner Workings
`LowerPySparkPlan` converts IR to recipe records such as `PySparkStepRecipe`, `PySparkJoinRecipe`, and
`PySparkExpressionRecipe`; renderer actions turn those recipes into schema modules, transform classes, runtime support,
and project file maps; `CompareGeneratedFiles` and `WriteGeneratedFiles` handle filesystem results without changing the
lowering model.
