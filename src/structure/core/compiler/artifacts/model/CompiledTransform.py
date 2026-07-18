from __future__ import annotations

from dataclasses import dataclass

from structure.core.compiler.artifacts.model.CompileKey import CompileKey
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.core.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan


@dataclass(frozen=True)
class CompiledTransform:
    key: CompileKey
    transform_plan: TransformPlan
    pyspark_plan: PySparkExecutionPlan
    schemas: TransformSchemas | None
    semantic_fingerprint: str
