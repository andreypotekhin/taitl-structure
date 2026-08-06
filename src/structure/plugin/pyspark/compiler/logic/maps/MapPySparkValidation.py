from structure.dsl import SchemaMode
from structure.plugin.api.v1.model import StepPlan, StepResultPlan
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


class MapPySparkValidation:

    def step(
        self,
        step: StepPlan,
        *,
        last: bool,
        check_intermediate: bool = True,
        boundary: bool = False,
    ) -> tuple[PySparkValidationRecipe, ...]:
        recipes = self._hooks(step.after_hooks, schema=step.output_schema)
        if not last:
            recipes.append(
                PySparkValidationRecipe(
                    target=step.results[0].frame,
                    schema=step.output_schema,
                    mode=self._intermediate_mode(step.after_hooks),
                    project=False,
                    reason="intermediate",
                    check=check_intermediate,
                    boundary=boundary,
                )
            )
        return tuple(recipes)

    def result(
        self,
        result: StepResultPlan,
        *,
        last: bool,
        check_intermediate: bool = True,
        boundary: bool = False,
    ) -> tuple[PySparkValidationRecipe, ...]:
        recipes = self._hooks(result.after_hooks, schema=result.schema)
        if not last:
            recipes.append(
                PySparkValidationRecipe(
                    target=result.frame,
                    schema=result.schema,
                    mode=self._intermediate_mode(result.after_hooks),
                    project=False,
                    reason="intermediate",
                    check=check_intermediate,
                    boundary=boundary,
                )
            )
        return tuple(recipes)

    def _hooks(self, hooks, *, schema) -> list[PySparkValidationRecipe]:
        recipes: list[PySparkValidationRecipe] = []
        for hook in hooks:
            recipes.append(
                PySparkValidationRecipe(
                    target=f"hook:{hook.name}",
                    schema=schema,
                    mode=hook.schema_mode,
                    project=hook.project_output,
                    reason="hook",
                )
            )
            if hook.project_output:
                recipes.append(
                    PySparkValidationRecipe(
                        target=f"hook:{hook.name}",
                        schema=schema,
                        mode=SchemaMode.STRICT,
                        project=False,
                        reason="hook_projected",
                    )
                )
        return recipes

    def _intermediate_mode(self, hooks) -> SchemaMode:
        if any(hook.schema_mode is SchemaMode.ALLOW_EXTRA_COLUMNS and not hook.project_output for hook in hooks):
            return SchemaMode.ALLOW_EXTRA_COLUMNS
        return SchemaMode.STRICT
