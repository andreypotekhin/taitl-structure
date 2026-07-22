from typing import cast

from structure.dsl import Schema
from structure.plugin.api.v1.model import BackendCapabilities, TransformPlan
from structure.plugin.pyspark.compiler.logic.mapping.PySparkInputMapper import PySparkInputMapper
from structure.plugin.pyspark.compiler.logic.mapping.PySparkOutputMapper import PySparkOutputMapper
from structure.plugin.pyspark.compiler.logic.mapping.PySparkStepMapper import PySparkStepMapper
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class LowerPySparkPlan:

    def __init__(self, capabilities: BackendCapabilities | None = None) -> None:
        self._capabilities = capabilities
        self._inputs = PySparkInputMapper()
        self._steps = PySparkStepMapper()
        self._outputs = PySparkOutputMapper()

    def __call__(
        self,
        plan: TransformPlan,
        *,
        capabilities: BackendCapabilities | None = None,
    ) -> PySparkExecutionPlan:
        target = capabilities or self._capabilities
        if target is None:
            raise ValueError("PySpark plan lowering requires explicit capabilities.")
        inputs = tuple(
            self._inputs.map(
                input.name,
                cast(type[Schema], input.schema),
                input.ordinal,
                cast(bool, input.streaming),
                input.aliases,
            )
            for input in plan.inputs
        )
        steps = tuple(
            self._steps.map(step, last=index == len(plan.steps) - 1, capabilities=target)
            for index, step in enumerate(plan.steps)
        )
        outputs = tuple(self._outputs.map(output, capabilities=target) for output in plan.outputs)
        return PySparkExecutionPlan(
            transform=plan.name,
            backend=target.id,
            inputs=inputs,
            steps=steps,
            outputs=outputs,
            requires_hook_inputs=False,
        )


lower_pyspark_plan = LowerPySparkPlan()
