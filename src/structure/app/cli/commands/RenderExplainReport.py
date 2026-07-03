from __future__ import annotations

from structure.app.compiler.api import Compiler
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe


class RenderExplainReport:

    def __call__(self, transform: type[Transform]) -> str:
        plan = Compiler.frontend.compile()(transform)
        recipe = PySpark.plan.lower()(plan)
        streaming = Compiler.compileability.streaming()(
            recipe,
            required=bool((plan.options or {}).get("streaming_compatible", False)),
        )
        source_transform = f"{transform.__module__}.{transform.__name__}"
        transform_module = f"{transform.__module__}.{recipe.transform}Generated"
        traceability = Compiler.traceability.build()(
            recipe,
            source_transform=source_transform,
            transform_module=transform_module,
        )
        lines = [
            recipe.transform,
            f"  backend: {recipe.backend.name} {recipe.backend.target}",
        ]
        if plan.diagnostics:
            lines.extend(["", "  diagnostics:"])
            for diagnostic in plan.diagnostics:
                lines.append(f"    {diagnostic.code}: {diagnostic.problem_text()}")
        lines.extend(
            [
                "",
                "  streaming:",
                f"    status: {streaming.support.value}",
                f"    required: {str(streaming.required).lower()}",
            ]
        )
        for finding in streaming.findings:
            lines.append(f"    {finding.code}: {finding.support.value} in {finding.step} ({finding.operation})")
        lines.extend(["", "  inputs:"])
        for item in recipe.inputs:
            lines.append(f"    {item.name}: {item.schema.__name__}")
        lines.extend(["", "  steps:"])
        for step, source_step in zip(recipe.steps, plan.steps, strict=True):
            outputs = (
                step.output_schema.__name__
                if len(step.results) == 1
                else ", ".join(f"{result.lane}: {result.schema.__name__}" for result in step.results)
            )
            lines.append(f"    {step.name}: {step.input_schema.__name__} -> {outputs}")
            if source_step.operations:
                operations = ", ".join(self._operation(operation) for operation in source_step.operations)
                lines.append(f"      operations: {operations}")
            if step.filters:
                lines.append(f"      filters: {len(step.filters)}")
            if step.joins:
                lines.append(f"      joins: {', '.join(self._join(join) for join in step.joins)}")
            hooks = [
                hook.name
                for hook in (
                    *step.before_hooks,
                    *step.after_hooks,
                    *(hook for result in step.results for hook in result.after_hooks if len(step.results) > 1),
                )
            ]
            if hooks:
                lines.append(f"      hooks: {', '.join(hooks)}")
            if step.validations:
                lines.append(f"      validations: {len(step.validations)}")
        lines.extend(["", "  traceability:"])
        for record in traceability.provenance[: min(4, len(traceability.provenance))]:
            lines.append(f"    {record.source} -> {record.ir} -> {record.generated}")
        lines.extend(["", "  static dataflow:"])
        for dependency in traceability.static_dataflow[: min(8, len(traceability.static_dataflow))]:
            sources = ", ".join(dependency.sources) if dependency.sources else "unknown"
            lines.append(f"    {dependency.target} <- {sources}")
        for boundary in traceability.opaque_boundaries:
            lines.append(f"    hook {boundary.hook}: opaque boundary {boundary.phase} {boundary.step}")
        if len(recipe.outputs) == 1:
            lines.extend(["", f"  output: {recipe.outputs[0].output_schema.__name__}"])
        else:
            lines.extend(["", "  outputs:"])
            for output in recipe.outputs:
                lines.append(f"    {output.name}: {output.output_schema.__name__}")
        return "\n".join(lines)

    def _operation(self, operation: OperationPlan) -> str:
        if operation.aggregate is not None:
            return f"aggregate({operation.cardinality.value} {self._aggregate(operation)})"
        if operation.selected_rows is not None:
            return (
                f"{operation.selected_rows.direction}_by("
                f"{operation.cardinality.value} partitions={len(operation.selected_rows.partition_by)})"
            )
        if operation.kind == "drop_duplicates":
            subset = 0 if operation.duplicate_rows is None else len(operation.duplicate_rows.subset)
            suffix = "" if not subset else f" subset={subset}"
            return f"drop_duplicates({operation.cardinality.value}{suffix})"
        name = operation.join.method.value if operation.join is not None else operation.kind
        return f"{name}({operation.cardinality.value})"

    def _aggregate(self, operation: OperationPlan) -> str:
        if operation.aggregate is None:
            return ""
        keys = ",".join(key.name for key in operation.aggregate.keys)
        metrics = ",".join(
            assignment.function for assignment in operation.aggregate.assignments if assignment.function != "key"
        )
        return f"keys={keys} metrics={metrics}"

    def _join(self, join: PySparkJoinRecipe) -> str:
        parts = [f"{join.input_name} {join.method.value} {self._cardinality(join)}"]
        if join.dedupe is not None:
            parts.append(f"dedupe={join.dedupe.direction}/{join.dedupe.ties.value}")
        if join.temporal is not None:
            parts.append(f"temporal=closed_open/{join.temporal.overlaps.value}")
        if join.as_of is not None:
            parts.append(f"as_of={join.as_of.direction.value}/{join.as_of.ties.value}")
        if join.hint is not None:
            parts.append(f"hint={join.hint.value}")
        if join.strategy is not None:
            parts.append(f"strategy={join.strategy.value}")
        return " ".join(parts)

    def _cardinality(self, join: PySparkJoinRecipe) -> str:
        if join.method.value in {"exists", "not_exists"}:
            return "row_filtering"
        if join.method.value == "join_many":
            return "row_multiplying"
        return "select_one"


render_explain_report = RenderExplainReport()
