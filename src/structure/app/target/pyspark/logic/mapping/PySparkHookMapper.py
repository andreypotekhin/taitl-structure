from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe


class PySparkHookMapper:

    def map(self, hook: HookPlan) -> PySparkHookRecipe:
        return PySparkHookRecipe(
            name=hook.name,
            phase=hook.phase,
            target=hook.target,
            pass_inputs=hook.pass_inputs,
            schema_mode=hook.schema_mode,
            project_output=hook.project_output,
            streaming_safe=hook.streaming_safe,
        )
