from collections import Counter
from typing import cast

from structure.dsl import Schema
from structure.plugin.api.v1.model import BackendCapabilities, TransformPlan
from structure.plugin.pyspark.compiler.commands.ValidatePySparkHooks import ValidatePySparkHooks
from structure.plugin.pyspark.compiler.commands.ValidatePySparkSchemaCapabilities import (
    ValidatePySparkSchemaCapabilities,
)
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkInput import MapPySparkInput
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkOutput import MapPySparkOutput
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkStep import MapPySparkStep
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStageOutputRecipe import PySparkStageOutputRecipe


class LowerPySparkPlan:

    def __init__(self, capabilities: BackendCapabilities | None = None) -> None:
        self._capabilities = capabilities
        self._inputs = MapPySparkInput()
        self._steps = MapPySparkStep()
        self._outputs = MapPySparkOutput()
        self._hooks = ValidatePySparkHooks()
        self._schema_capabilities = ValidatePySparkSchemaCapabilities()

    def __call__(
        self,
        plan: TransformPlan,
        *,
        capabilities: BackendCapabilities | None = None,
        check_intermediate: bool = True,
        boundary_policy: str = "off",
    ) -> PySparkExecutionPlan:
        target = capabilities or self._capabilities
        if target is None:
            raise ValueError("PySpark plan lowering requires explicit capabilities.")
        self._hooks(plan)
        self._schema_capabilities(plan, capabilities=target)
        inputs = tuple(
            self._inputs.map(
                input.name,
                cast(type[Schema], input.schema),
                ordinal,
                cast(bool, input.streaming),
                input.aliases,
                input.optional,
                internal,
            )
            for ordinal, (input, internal) in enumerate(
                (*((input, False) for input in plan.inputs), *((input, True) for input in plan.internal_inputs))
            )
        )
        boundary_frames = self._boundary_frames(plan)
        steps = tuple(
            self._steps.map(
                step,
                last=index == len(plan.steps) - 1,
                capabilities=target,
                check_intermediate=check_intermediate,
                boundary_policy=boundary_policy,
                boundary_frames=boundary_frames,
            )
            for index, step in enumerate(plan.steps)
        )
        outputs = tuple(self._outputs.map(output, capabilities=target) for output in plan.outputs)
        stage_outputs = tuple(
            PySparkStageOutputRecipe(
                path=stage_output.path,
                output=self._outputs.map(stage_output.output, capabilities=target),
            )
            for stage_output in plan.stage_outputs
        )
        return PySparkExecutionPlan(
            transform=plan.name,
            backend=target.id,
            inputs=inputs,
            steps=steps,
            outputs=outputs,
            requires_hook_inputs=False,
            stage_outputs=stage_outputs,
            allow_stage_outputs=plan.allow_stage_outputs,
        )

    @staticmethod
    def _boundary_frames(plan: TransformPlan) -> frozenset[str]:
        consumers = Counter(input.source for step in plan.steps for input in step.inputs)
        consumers.update(output.source for output in plan.outputs)
        return frozenset(frame for frame, count in consumers.items() if count >= 2)


lower_pyspark_plan = LowerPySparkPlan()
