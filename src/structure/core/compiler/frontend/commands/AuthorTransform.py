from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.platform.api.v1 import AuthoringAPI


class AuthorTransform:
    """Run Core's step lifecycle and attach bodies captured by one selected platform.

    ``CompileTransform`` remains the temporary implementation of the PySpark-shaped
    body construction while P072 moves those builders into the plugin.  This command
    deliberately exposes only the captured body to its caller: the structural plan
    supplied by :class:`AnalyzeTransform` remains the plan sent to the compiler.
    """

    def __init__(self) -> None:
        self._legacy = CompileTransform()

    def __call__(
        self,
        transform: type[Transform] | TransformPipeline,
        plan: TransformPlan,
        *,
        config: StructureConfig,
        authoring: AuthoringAPI,
        target: str,
        configuration: Mapping[str, object],
    ) -> TransformPlan:
        authored = self._legacy(
            transform,
            config=config,
            _authoring=authoring,
            _authoring_target=target,
            _authoring_configuration=configuration,
        )
        if len(plan.steps) != len(authored.steps) or tuple(step.name for step in plan.steps) != tuple(
            step.name for step in authored.steps
        ):
            # Pipeline body rewrites are still performed by the legacy compatibility
            # path.  A subsequent P072 change authors only after structural pipeline
            # composition and removes this escape hatch.
            return authored
        return replace(
            plan,
            steps=tuple(
                replace(structural, platform_body=captured.platform_body)
                for structural, captured in zip(plan.steps, authored.steps, strict=True)
            ),
            diagnostics=(*plan.diagnostics, *authored.diagnostics),
        )
