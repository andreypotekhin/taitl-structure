from structure.app.compiler.ir.model.StepPlan import StepPlan
from structure.app.compiler.ir.model.StepResultPlan import StepResultPlan
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


class PySparkValidationMapper:

    def step(self, step: StepPlan, *, last: bool) -> tuple[PySparkValidationRecipe, ...]:
        recipes = self._hooks(step.after_hooks, schema=step.output_schema)
        if not last:
            recipes.append(
                PySparkValidationRecipe(
                    target=step.name,
                    schema=step.output_schema,
                    mode=SchemaMode.STRICT,
                    project=False,
                    reason="intermediate",
                )
            )
        return tuple(recipes)

    def result(self, result: StepResultPlan, *, last: bool) -> tuple[PySparkValidationRecipe, ...]:
        recipes = self._hooks(result.after_hooks, schema=result.schema)
        if not last:
            recipes.append(
                PySparkValidationRecipe(
                    target=result.frame,
                    schema=result.schema,
                    mode=SchemaMode.STRICT,
                    project=False,
                    reason="intermediate",
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
