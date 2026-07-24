from structure.plugin.api.v1.model import HookPlan
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe


class MapPySparkHook:

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
            streaming=hook.streaming,
            target_backend=hook.target_backend,
            target_defaulted=hook.target_defaulted,
            target_platform=hook.target_platform,
            origin=hook.origin,
        )
