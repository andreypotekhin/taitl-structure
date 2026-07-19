from structure.core.compiler.ir.model.HookPlan import HookPlan
from structure.platform.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe


class PySparkHookMapper:

    def map(self, hook: HookPlan) -> PySparkHookRecipe:
        return PySparkHookRecipe(
            name=hook.name,
            phase=hook.phase,
            target=hook.target,
            lanes=tuple(lane.name for lane in hook.lanes),
            outputs=tuple(output.name for output in hook.outputs),
            sources=hook.sources,
            schema_mode=hook.schema_mode,
            project_output=hook.project_output,
            streaming_safe=hook.streaming_safe,
            target_backend=hook.target_backend,
            target_defaulted=hook.target_defaulted,
            target_platform=hook.target_platform,
            origin=hook.origin,
        )
