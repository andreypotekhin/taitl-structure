from __future__ import annotations

from types import MappingProxyType

from structure.app.backend.pyspark.logic.actions.MaterializePySparkSchema import materialize_pyspark_schema
from structure.app.backend.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.runtime.logic.model.TransformSchemas import TransformSchemas


class BuildTransformSchemas:

    def __call__(self, plan: PySparkExecutionPlan, *, types=None) -> TransformSchemas:
        inputs = {input.name: materialize_pyspark_schema(input.schema, types=types) for input in plan.inputs}
        steps = {step.name: materialize_pyspark_schema(step.output_schema, types=types) for step in plan.steps}
        output = materialize_pyspark_schema(plan.final_validation.schema, types=types)
        return TransformSchemas(
            inputs=MappingProxyType(inputs),
            steps=MappingProxyType(steps),
            output=output,
        )


build_transform_schemas = BuildTransformSchemas()
