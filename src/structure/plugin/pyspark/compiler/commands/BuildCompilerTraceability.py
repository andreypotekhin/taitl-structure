from __future__ import annotations

from structure.plugin.api.v1.model import CompilerProvenance, CompilerTraceability, DataflowDependency, OpaqueBoundary
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.logic.traceability.FindPythonUdfBoundaries import FindPythonUdfBoundaries
from structure.plugin.pyspark.compiler.logic.traceability.MapAggregateTraceability import MapAggregateTraceability
from structure.plugin.pyspark.compiler.logic.traceability.MapDeduplicationTraceability import (
    MapDeduplicationTraceability,
)
from structure.plugin.pyspark.compiler.logic.traceability.MapFilterTraceability import MapFilterTraceability
from structure.plugin.pyspark.compiler.logic.traceability.MapHookTraceability import MapHookTraceability
from structure.plugin.pyspark.compiler.logic.traceability.MapJoinTraceability import MapJoinTraceability
from structure.plugin.pyspark.compiler.logic.traceability.MapProjectionTraceability import MapProjectionTraceability
from structure.plugin.pyspark.compiler.logic.traceability.MapRelationAssertionTraceability import (
    MapRelationAssertionTraceability,
)
from structure.plugin.pyspark.compiler.logic.traceability.MapSelectedRowsTraceability import MapSelectedRowsTraceability
from structure.plugin.pyspark.compiler.logic.traceability.MapValidationTraceability import MapValidationTraceability
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class BuildCompilerTraceability:

    def __init__(self) -> None:
        self._dataflow = CompilerDataflowReads()
        self._aggregates = MapAggregateTraceability(self._dataflow)
        self._deduplication = MapDeduplicationTraceability(self._dataflow)
        self._filters = MapFilterTraceability(self._dataflow)
        self._hooks = MapHookTraceability()
        self._joins = MapJoinTraceability(self._dataflow)
        self._projections = MapProjectionTraceability(self._dataflow)
        self._relation_assertions = MapRelationAssertionTraceability()
        self._selected_rows = MapSelectedRowsTraceability(self._dataflow)
        self._udf_boundaries = FindPythonUdfBoundaries()
        self._validations = MapValidationTraceability()

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
            provenance.extend(self._filters.provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._filters.dependencies(step))
            provenance.extend(self._joins.provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._joins.dependencies(step))
            provenance.extend(self._aggregates.provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._aggregates.dependencies(step))
            provenance.extend(self._aggregates.having_provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._aggregates.having_dependencies(step))
            provenance.extend(self._selected_rows.provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._selected_rows.dependencies(step))
            provenance.extend(self._deduplication.provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._deduplication.dependencies(step))
            provenance.extend(self._relation_assertions.provenance(plan, step, source_transform, transform_module))
            dependencies.extend(self._relation_assertions.dependencies(step))
            if len(step.results) <= 1:
                provenance.extend(self._projections.provenance(plan, step, source_transform, transform_module))
                dependencies.extend(self._projections.dependencies(step))
                boundaries.extend(
                    self._udf_boundaries(
                        step=step.name,
                        schema=step.output_schema.__name__,
                        expressions=(assignment.expression for assignment in step.projection),
                    )
                )
            else:
                for result in step.results:
                    provenance.extend(
                        self._projections.result_provenance(plan, step, result, source_transform, transform_module)
                    )
                    dependencies.extend(self._projections.result_dependencies(step, result))
                    boundaries.extend(
                        self._udf_boundaries(
                            step=step.name,
                            schema=result.schema.__name__,
                            expressions=(assignment.expression for assignment in result.projection),
                        )
                    )
                    for hook in result.after_hooks:
                        provenance.append(self._hooks.provenance(plan, step, hook, source_transform, transform_module))
                        dependencies.append(self._hooks.dependency(step, hook))
                        boundaries.append(self._hooks.boundary(step, hook, result.schema.__name__))

            for hook in (*step.before_hooks, *step.after_hooks):
                provenance.append(self._hooks.provenance(plan, step, hook, source_transform, transform_module))
                dependencies.append(self._hooks.dependency(step, hook))
                boundaries.append(self._hooks.boundary(step, hook, step.output_schema.__name__))

            provenance.extend(self._validations.provenance(plan, step, source_transform, transform_module))
            previous = step.output_schema.__name__

        for output in plan.outputs:
            provenance.append(self._validations.final_provenance(plan, output, source_transform, transform_module))
            dependencies.append(self._validations.final_dependency(output.validation))
            boundaries.extend(
                self._udf_boundaries(
                    step=f"output:{output.name}",
                    schema=output.output_schema.__name__,
                    expressions=(assignment.expression for assignment in output.projection),
                )
            )
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


build_compiler_traceability = BuildCompilerTraceability()
