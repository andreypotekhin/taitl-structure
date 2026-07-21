from __future__ import annotations

from structure.dsl import StreamingMode, Transform
from structure.plugin.api.v1.model import ExplainRequest, TransformPlan
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.dsl.joins import Join, JoinMethod


class RenderPySparkExplainReport:

    @property
    def _streaming(self):
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark.compiler.streaming()

    @property
    def _traceability(self):
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark.compiler.traceability()

    def __call__(self, request: ExplainRequest) -> str:
        transform = request.transform
        if not isinstance(transform, type) or not issubclass(transform, Transform):
            raise TypeError("PySpark explain rendering requires a Transform class.")
        plan = request.analysis
        recipe = request.payload
        if not isinstance(plan, TransformPlan) or not isinstance(recipe, PySparkExecutionPlan):
            raise ValueError("PLUGIN-E2708: PySpark explain rendering requires compiled analysis and payload.")
        streaming = self._streaming(
            recipe,
            required=bool((plan.options or {}).get("streaming_compatible", False)),
        )
        source_transform = f"{transform.__module__}.{transform.__name__}"
        transform_module = f"{transform.__module__}.{recipe.transform}Generated"
        traceability = self._traceability(
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
        input_modes = {input.name: input.streaming for input in recipe.inputs}
        for step in recipe.steps:
            outputs = (
                step.output_schema.__name__
                if len(step.results) == 1
                else ", ".join(f"{result.lane}: {result.schema.__name__}" for result in step.results)
            )
            lines.append(f"    {step.name}: {step.input_schema.__name__} -> {outputs}")
            if step.operations:
                operations = ", ".join(self._operation(operation) for operation in step.operations)
                lines.append(f"      operations: {operations}")
            if step.filters:
                lines.append(f"      filters: {len(step.filters)}")
            if step.joins:
                lines.append(
                    f"      joins: {', '.join(self._join(join, current=step.source, input_modes=input_modes) for join in step.joins)}"
                )
            helpers = self._collection_helpers(
                assignment.expression
                for assignment in (
                    *step.projection,
                    *(assignment for result in step.results for assignment in result.projection),
                )
            )
            if helpers:
                lines.append(f"      collection helpers: {', '.join(helpers)}")
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

    def _collection_helpers(self, expressions) -> tuple[str, ...]:
        helpers: list[str] = []
        for expression in expressions:
            for helper in self._helper_expressions(expression):
                data = helper.data or {}
                if data.get("capability_group") != "higher_order":
                    continue
                name = str(data["function"])
                fields = self._fields(helper)
                detail = ",".join(fields[:2])
                rendered = f"{name}({detail})" if detail else name
                if rendered not in helpers:
                    helpers.append(rendered)
        return tuple(helpers)

    def _helper_expressions(self, expression) -> tuple:
        nested = [expression] if expression.kind == "transform_expression" else []
        for argument in expression.args:
            nested.extend(self._helper_expressions(argument))
        return tuple(nested)

    def _fields(self, expression) -> tuple[str, ...]:
        fields: list[str] = []
        if expression.kind == "field":
            data = expression.data or {}
            fields.append(str(data.get("name", data.get("field", "field"))))
        for argument in expression.args:
            fields.extend(self._fields(argument))
        return tuple(fields)

    def _operation(self, operation: PySparkOperationRecipe) -> str:
        if operation.aggregate is not None:
            return f"aggregate(aggregate {self._aggregate(operation)})"
        if operation.selected_rows is not None:
            return (
                f"{operation.selected_rows.direction}_by("
                f"select_one partitions={len(operation.selected_rows.partition_by)})"
            )
        if operation.kind == "drop_duplicates":
            subset = 0 if operation.duplicate_rows is None else len(operation.duplicate_rows.subset)
            scope = None if operation.duplicate_rows is None else operation.duplicate_rows.scope
            suffix = "" if not subset else f" subset={subset}"
            if scope is not None:
                suffix = f"{suffix} scope={scope}"
            return f"drop_duplicates(row_filtering{suffix}{self._streaming_modes(operation)})"
        if operation.watermark is not None:
            return f"watermark({operation.watermark.column} {operation.watermark.delay})"
        if operation.filter is not None:
            return "filter(row_filtering)"
        name = operation.join.method.value if operation.join is not None else operation.kind
        return f"{name}({self._operation_cardinality(operation)})"

    def _aggregate(self, operation: PySparkOperationRecipe) -> str:
        if operation.aggregate is None:
            return ""
        keys = ",".join(key.name for key in operation.aggregate.keys)
        levels = self._aggregate_levels(operation)
        metrics = ",".join(
            assignment.function for assignment in operation.aggregate.assignments if assignment.function != "key"
        )
        having = " having=1" if operation.aggregate.having is not None else ""
        return f"keys={keys}{levels} metrics={metrics}{having}{self._streaming_modes(operation)}"

    def _aggregate_levels(self, operation: PySparkOperationRecipe) -> str:
        if operation.aggregate is None or not operation.aggregate.levels:
            return ""
        levels = "|".join("+".join(level) if level else "()" for level in operation.aggregate.levels)
        return f" levels={levels}"

    def _operation_cardinality(self, operation: PySparkOperationRecipe) -> str:
        if operation.kind == "cache":
            return "row_preserving"
        if operation.join is None:
            return "unknown"
        return {
            JoinMethod.LOOKUP: "select_one",
            JoinMethod.EXISTS: "row_filtering",
            JoinMethod.NOT_EXISTS: "row_filtering",
            JoinMethod.ROWSET: "row_multiplying",
            JoinMethod.TEMPORAL_ONE: "select_one",
            JoinMethod.AS_OF_ONE: "select_one",
        }[operation.join.method]

    @staticmethod
    def _streaming_modes(operation: PySparkOperationRecipe) -> str:
        if not operation.streaming_output_modes:
            return ""
        return f" streaming_modes={'|'.join(mode.value for mode in operation.streaming_output_modes)}"

    def _join(
        self,
        join: PySparkJoinRecipe,
        *,
        current: str,
        input_modes: dict[str, StreamingMode],
    ) -> str:
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
        if self._requires_append(join, current=current, input_modes=input_modes):
            parts.append("streaming_modes=append")
        return " ".join(parts)

    def _requires_append(
        self,
        join: PySparkJoinRecipe,
        *,
        current: str,
        input_modes: dict[str, StreamingMode],
    ) -> bool:
        if input_modes.get(current) is not StreamingMode.YES or input_modes.get(join.source) is not StreamingMode.YES:
            return False
        return join.method is JoinMethod.EXISTS or (
            join.method is JoinMethod.ROWSET and join.how in {Join.LEFT, Join.RIGHT, Join.FULL}
        )

    def _cardinality(self, join: PySparkJoinRecipe) -> str:
        if join.method.value in {"exists", "not_exists"}:
            return "row_filtering"
        if join.method is JoinMethod.ROWSET:
            return "row_multiplying"
        return "select_one"


render_pyspark_explain_report = RenderPySparkExplainReport()
