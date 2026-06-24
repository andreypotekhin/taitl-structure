from structure.app.compiler.ir.model.TransformPlan import TransformPlan
from structure.app.target.capabilities.api import Capabilities
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.pyspark.logic.mapping.PySparkInputMapper import PySparkInputMapper
from structure.app.target.pyspark.logic.mapping.PySparkOutputMapper import PySparkOutputMapper
from structure.app.target.pyspark.logic.mapping.PySparkStepMapper import PySparkStepMapper
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan


class LowerPySparkPlan:

    def __init__(self) -> None:
        self._inputs = PySparkInputMapper()
        self._steps = PySparkStepMapper()
        self._outputs = PySparkOutputMapper()

    def __call__(
        self,
        plan: TransformPlan,
        *,
        capabilities: BackendCapabilities | None = None,
    ) -> PySparkExecutionPlan:
        target = capabilities or Capabilities.resolve()()
        inputs = tuple(self._inputs.map(input.name, input.schema, input.ordinal) for input in plan.inputs)
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
            requires_hook_inputs=any(
                hook.pass_inputs
                for step in steps
                for hook in (
                    *step.before_hooks,
                    *step.after_hooks,
                    *(hook for result in step.results for hook in result.after_hooks),
                )
            ),
        )


lower_pyspark_plan = LowerPySparkPlan()
