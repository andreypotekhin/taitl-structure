from __future__ import annotations

from collections.abc import Iterable

from structure.app.compiler.traceability.logic.model.CompilerProvenance import CompilerProvenance
from structure.app.compiler.traceability.logic.model.CompilerTraceability import CompilerTraceability
from structure.app.compiler.traceability.logic.model.DataflowDependency import DataflowDependency
from structure.app.compiler.traceability.logic.model.OpaqueBoundary import OpaqueBoundary
from structure.app.target.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.logic.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.target.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe


class BuildCompilerTraceability:

    def __call__(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        transform_module: str,
    ) -> CompilerTraceability:
        provenance = [self._transform_provenance(plan, source_transform, transform_module)]
        dependencies = [self._transform_dependency(plan)]
        boundaries: list[OpaqueBoundary] = []

        for item in plan.inputs:
            provenance.append(
                CompilerProvenance(
                    source=f"source:{source_transform}.input.{item.name}",
                    ir=f"ir:{plan.transform}.input.{item.ordinal}.{item.name}",
                    generated=f"generated:{transform_module}.{plan.transform}Generated.run.input.{item.name}",
                )
            )

        previous = plan.inputs[0].name if plan.inputs else "input"
        for step in plan.steps:
            provenance.append(self._step_provenance(plan, step, source_transform, transform_module))
            dependencies.append(self._step_dependency(step, previous))
            provenance.extend(self._filter_provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._filter_dependencies(step))
            provenance.extend(self._join_provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._join_dependencies(step))
            if len(step.results) <= 1:
                provenance.extend(self._projection_provenance(plan, step, source_transform, transform_module))
                dependencies.extend(self._projection_dependencies(step))
            else:
                for result in step.results:
                    provenance.extend(
                        CompilerProvenance(
                            source=f"source:{source_transform}.{step.name}.result.{result.lane}.{assignment.field.name}",
                            ir=(
                                f"ir:{plan.transform}.step.{step.ordinal}.{step.name}."
                                f"result.{result.ordinal}.{result.lane}.project.{assignment.field.name}"
                            ),
                            generated=(
                                f"generated:{transform_module}.{plan.transform}Generated.run."
                                f"step.{step.ordinal}.{step.name}.{result.lane}.select.{assignment.field.name}"
                            ),
                        )
                        for assignment in result.projection
                    )
                    dependencies.extend(
                        DataflowDependency(
                            target=f"{result.lane}.{assignment.field.name}",
                            sources=self._reads(assignment.expression) or ("literal",),
                            operation="project",
                            step=step.name,
                            detail={"field": assignment.field.name, "result": result.lane},
                        )
                        for assignment in result.projection
                    )
                    for hook in result.after_hooks:
                        provenance.append(self._hook_provenance(plan, step, hook, source_transform, transform_module))
                        dependencies.append(self._hook_dependency(step, hook))
                        boundaries.append(
                            OpaqueBoundary(
                                step=step.name,
                                hook=hook.name,
                                phase=hook.phase,
                                target=hook.target,
                                schema=result.schema.__name__,
                                reason="arbitrary PySpark hook body",
                            )
                        )

            for hook in (*step.before_hooks, *step.after_hooks):
                provenance.append(self._hook_provenance(plan, step, hook, source_transform, transform_module))
                dependencies.append(self._hook_dependency(step, hook))
                boundaries.append(
                    OpaqueBoundary(
                        step=step.name,
                        hook=hook.name,
                        phase=hook.phase,
                        target=hook.target,
                        schema=step.output_schema.__name__,
                        reason="arbitrary PySpark hook body",
                    )
                )

            provenance.extend(self._validation_provenance(plan, step, source_transform, transform_module))
            previous = step.output_schema.__name__

        for output in plan.outputs:
            provenance.append(
                CompilerProvenance(
                    source=f"source:{source_transform}.output.{output.name}",
                    ir=f"ir:{plan.transform}.output.{output.ordinal}.{output.name}.validation.final",
                    generated=(
                        f"generated:{transform_module}.{plan.transform}Generated.run."
                        f"output.{output.ordinal}.{output.name}.validation.final"
                    ),
                )
            )
            dependencies.append(self._final_validation_dependency(output.validation))
        return CompilerTraceability(
            provenance=tuple(provenance),
            static_dataflow=tuple(dependencies),
            opaque_boundaries=tuple(boundaries),
        )

    def _transform_provenance(
        self,
        plan: PySparkExecutionPlan,
        source_transform: str,
        transform_module: str,
    ) -> CompilerProvenance:
        return CompilerProvenance(
            source=f"source:{source_transform}",
            ir=f"ir:{plan.transform}",
            generated=f"generated:{transform_module}.{plan.transform}Generated",
        )

    def _step_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> CompilerProvenance:
        return CompilerProvenance(
            source=f"source:{source_transform}.{step.name}",
            ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}",
            generated=f"generated:{transform_module}.{plan.transform}Generated.run.step.{step.ordinal}.{step.name}",
        )

    def _filter_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.filter.{index}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.filter.{index}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.where.{index}"
                ),
            )
            for index, _ in enumerate(step.filters)
        )

    def _join_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.join.{join.occurrence}.{join.input_name}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.join.{join.occurrence}.{join.input_name}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.join.{join.occurrence}.{join.input_name}"
                ),
            )
            for join in step.joins
        )

    def _projection_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.field.{assignment.field.name}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.project.{assignment.field.name}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.select.{assignment.field.name}"
                ),
            )
            for assignment in step.projection
        )

    def _hook_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        hook: PySparkHookRecipe,
        source_transform: str,
        transform_module: str,
    ) -> CompilerProvenance:
        return CompilerProvenance(
            source=f"source:{source_transform}.{hook.name}",
            ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.hook.{hook.phase}.{hook.name}",
            generated=(
                f"generated:{transform_module}.{plan.transform}Generated.run."
                f"step.{step.ordinal}.{step.name}.hook.{hook.phase}.{hook.name}"
            ),
        )

    def _validation_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.validation.{index}.{validation.reason}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.validation.{index}.{validation.reason}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.validation.{index}.{validation.reason}"
                ),
            )
            for index, validation in enumerate(step.validations)
        )

    def _final_validation_provenance(
        self,
        plan: PySparkExecutionPlan,
        source_transform: str,
        transform_module: str,
    ) -> CompilerProvenance:
        return CompilerProvenance(
            source=f"source:{source_transform}.output",
            ir=f"ir:{plan.transform}.validation.final",
            generated=f"generated:{transform_module}.{plan.transform}Generated.run.validation.final",
        )

    def _transform_dependency(self, plan: PySparkExecutionPlan) -> DataflowDependency:
        return DataflowDependency(
            target=plan.transform,
            sources=tuple(item.name for item in plan.inputs),
            operation="transform",
            step=None,
            detail={"backend": plan.backend.name},
        )

    def _step_dependency(self, step: PySparkStepRecipe, previous: str) -> DataflowDependency:
        return DataflowDependency(
            target=step.name,
            sources=(previous, *tuple(join.input_name for join in step.joins)),
            operation="step",
            step=step.name,
            detail={"input_schema": step.input_schema.__name__, "output_schema": step.output_schema.__name__},
        )

    def _filter_dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.filter[{index}]",
                sources=self._reads(filter),
                operation="filter",
                step=step.name,
                detail={},
            )
            for index, filter in enumerate(step.filters)
        )

    def _join_dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.join[{join.occurrence}].{join.input_name}",
                sources=self._reads(join.predicate),
                operation="join_one",
                step=step.name,
                detail={
                    "hint": join.hint.value if join.hint is not None else None,
                    "how": join.how.value,
                    "right_alias": join.right_alias,
                },
            )
            for join in step.joins
        )

    def _projection_dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.output_schema.__name__}.{assignment.field.name}",
                sources=self._reads(assignment.expression) or ("literal",),
                operation="project",
                step=step.name,
                detail={"field": assignment.field.name},
            )
            for assignment in step.projection
        )

    def _hook_dependency(self, step: PySparkStepRecipe, hook: PySparkHookRecipe) -> DataflowDependency:
        return DataflowDependency(
            target=f"{step.name}.hook.{hook.name}",
            sources=(step.input_schema.__name__ if hook.phase == "before" else step.output_schema.__name__,),
            operation="hook",
            step=step.name,
            detail={
                "phase": hook.phase,
                "project_output": hook.project_output,
                "schema_mode": hook.schema_mode.value,
            },
        )

    def _final_validation_dependency(self, validation: PySparkValidationRecipe) -> DataflowDependency:
        return DataflowDependency(
            target=f"{validation.schema.__name__}.validation.final",
            sources=(validation.schema.__name__,),
            operation="validate_schema",
            step=None,
            detail={"mode": validation.mode.value},
        )

    def _reads(self, expression: PySparkExpressionRecipe) -> tuple[str, ...]:
        if expression.kind == "field":
            return (f"{expression.data['scope']}.{expression.data['field']}",)
        return self._unique(read for argument in expression.args for read in self._reads(argument))

    def _unique(self, values: Iterable[str]) -> tuple[str, ...]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value not in seen:
                result.append(value)
                seen.add(value)
        return tuple(result)


build_compiler_traceability = BuildCompilerTraceability()
