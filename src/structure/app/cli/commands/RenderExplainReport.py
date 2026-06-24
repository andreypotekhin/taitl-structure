from __future__ import annotations

from structure.app.compiler.api import Compiler
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.target.pyspark.api import PySpark


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
        for step in recipe.steps:
            outputs = (
                step.output_schema.__name__
                if len(step.results) == 1
                else ", ".join(f"{result.lane}: {result.schema.__name__}" for result in step.results)
            )
            lines.append(f"    {step.name}: {step.input_schema.__name__} -> {outputs}")
            if step.filters:
                lines.append(f"      filters: {len(step.filters)}")
            if step.joins:
                lines.append(f"      joins: {', '.join(join.input_name for join in step.joins)}")
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


render_explain_report = RenderExplainReport()
