from structure.plugin.api.v1.model import HookPlan
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


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
            targets=hook.targets,
            target_defaulted=hook.target_defaulted,
            target_platform=hook.target_platform,
            origin=hook.origin,
            validations=tuple(
                PySparkValidationRecipe(
                    target=output.name,
                    schema=output.schema,
                    mode=hook.schema_mode,
                    project=hook.project_output,
                    reason="hook",
                )
                for output in hook.outputs
            ),
        )
