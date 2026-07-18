from __future__ import annotations

from types import MappingProxyType

from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.core.target.pyspark.api import PySpark
from structure.core.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan


class BuildTransformSchemas:

    def __call__(self, plan: PySparkExecutionPlan, *, types=None) -> TransformSchemas:
        materialize = PySpark.schema.materialize()
        inputs = {input.name: materialize(input.schema, types=types) for input in plan.inputs}
        steps = {step.name: materialize(step.output_schema, types=types) for step in plan.steps}
        outputs = {output.name: materialize(output.output_schema, types=types) for output in plan.outputs}
        output_aliases = {output.name: output.aliases for output in plan.outputs if output.aliases}
        return TransformSchemas(
            inputs=MappingProxyType(inputs),
            steps=MappingProxyType(steps),
            outputs=MappingProxyType(outputs),
            output_aliases=MappingProxyType(output_aliases),
        )


build_transform_schemas = BuildTransformSchemas()
