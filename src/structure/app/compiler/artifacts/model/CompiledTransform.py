from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.artifacts.model.CompileKey import CompileKey
from structure.app.compiler.ir.model.TransformPlan import TransformPlan
from structure.app.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan


@dataclass(frozen=True)
class CompiledTransform:
    key: CompileKey
    transform_plan: TransformPlan
    pyspark_plan: PySparkExecutionPlan
    schemas: TransformSchemas

