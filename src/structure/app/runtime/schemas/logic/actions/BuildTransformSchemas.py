from __future__ import annotations

from types import MappingProxyType

from structure.app.runtime.schemas.logic.model.TransformSchemas import TransformSchemas
from structure.app.target.pyspark.logic.actions.MaterializePySparkSchema import materialize_pyspark_schema
from structure.app.target.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan


class BuildTransformSchemas:

    def __call__(self, plan: PySparkExecutionPlan, *, types=None) -> TransformSchemas:
        inputs = {input.name: materialize_pyspark_schema(input.schema, types=types) for input in plan.inputs}
        steps = {step.name: materialize_pyspark_schema(step.output_schema, types=types) for step in plan.steps}
        outputs = {
            output.name: materialize_pyspark_schema(output.output_schema, types=types) for output in plan.outputs
        }
        return TransformSchemas(
            inputs=MappingProxyType(inputs),
            steps=MappingProxyType(steps),
            outputs=MappingProxyType(outputs),
        )


build_transform_schemas = BuildTransformSchemas()
