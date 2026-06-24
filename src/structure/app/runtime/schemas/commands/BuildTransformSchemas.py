from __future__ import annotations

from types import MappingProxyType

from structure.app.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan


class BuildTransformSchemas:

    def __call__(self, plan: PySparkExecutionPlan, *, types=None) -> TransformSchemas:
        materialize = PySpark.schema.materialize()
        inputs = {input.name: materialize(input.schema, types=types) for input in plan.inputs}
        steps = {step.name: materialize(step.output_schema, types=types) for step in plan.steps}
        outputs = {output.name: materialize(output.output_schema, types=types) for output in plan.outputs}
        return TransformSchemas(
            inputs=MappingProxyType(inputs),
            steps=MappingProxyType(steps),
            outputs=MappingProxyType(outputs),
        )


build_transform_schemas = BuildTransformSchemas()
