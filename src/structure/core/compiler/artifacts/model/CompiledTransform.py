from __future__ import annotations

from dataclasses import dataclass

from structure.core.compiler.artifacts.model.CompileKey import CompileKey
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.plugin.api.v1.model.OptimizationReport import OptimizationReport


@dataclass(frozen=True)
class CompiledTransform:
    key: CompileKey
    transform_plan: TransformPlan
    payload: object
    schemas: TransformSchemas | None
    semantic_fingerprint: str
    optimization: OptimizationReport | None = None

    @property
    def pyspark_plan(self) -> object:
        return self.payload
