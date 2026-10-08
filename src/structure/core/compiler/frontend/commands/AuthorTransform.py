from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.plugin.api.v1 import AuthoringAPI


class AuthorTransform:
    """Run Core's step lifecycle and attach bodies captured by one selected plugin.

    ``CompileTransform`` remains the temporary implementation of the PySpark-shaped
    body construction while P072 moves those builders into the plugin.  This command
    deliberately exposes only the captured body to its caller: the structural plan
    supplied by :class:`AnalyzeTransform` remains the plan sent to the compiler.
    """

    def __init__(self) -> None:
        self._legacy = CompileTransform()

    def __call__(
        self,
        transform: type[Transform] | Transform | TransformPipeline,
        plan: TransformPlan,
        *,
        config: StructureConfig,
        authoring: AuthoringAPI,
        target: str,
        configuration: Mapping[str, object],
        plugin_options: Mapping[str, object],
    ) -> TransformPlan:
        authored = self._legacy(
            transform,
            config=config,
            _authoring=authoring,
            _authoring_target=target,
            _authoring_configuration=configuration,
            _authoring_plugin_options=plugin_options,
        )
        if len(plan.steps) != len(authored.steps) or tuple(step.name for step in plan.steps) != tuple(
            step.name for step in authored.steps
        ):
            # Pipeline body rewrites are still performed by the legacy compatibility
            # path.  A subsequent P072 change authors only after structural pipeline
            # composition and removes this escape hatch.
            return authored
        authored_stage_outputs = {item.path: item.output for item in authored.stage_outputs}
        return replace(
            plan,
            steps=tuple(
                replace(
                    structural,
                    results=tuple(
                        replace(
                            result,
                            binding=authored_result.binding,
                            table_source=authored_result.table_source,
                        )
                        for result, authored_result in zip(structural.results, captured.results, strict=True)
                    ),
                    plugin_body=captured.plugin_body,
                    sinks=captured.sinks,
                    effect=captured.effect,
                )
                for structural, captured in zip(plan.steps, authored.steps, strict=True)
            ),
            outputs=tuple(
                replace(
                    output,
                    binding=authored_output.binding,
                    table_source=authored_output.table_source,
                )
                for output, authored_output in zip(plan.outputs, authored.outputs, strict=True)
            ),
            stage_outputs=tuple(
                replace(
                    stage_output,
                    output=replace(
                        stage_output.output,
                        binding=authored_stage_outputs[stage_output.path].binding,
                        table_source=authored_stage_outputs[stage_output.path].table_source,
                    ),
                )
                if stage_output.path in authored_stage_outputs
                else stage_output
                for stage_output in plan.stage_outputs
            ),
            sinks=authored.sinks,
            diagnostics=(*plan.diagnostics, *authored.diagnostics),
        )
