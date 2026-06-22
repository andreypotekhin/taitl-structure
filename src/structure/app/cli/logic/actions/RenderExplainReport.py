from __future__ import annotations

from structure.app.backend.pyspark.api import lower_pyspark_plan
from structure.app.compiler.traceability.api import build_compiler_traceability
from structure.app.dsl.api import compile_transform
from structure.app.dsl.logic.model.transforms.Transform import Transform
from structure.app.streaming.api import classify_streaming_compatibility


class RenderExplainReport:

    def __call__(self, transform: type[Transform]) -> str:
        plan = compile_transform(transform)
        recipe = lower_pyspark_plan(plan)
        streaming = classify_streaming_compatibility(
            recipe,
            required=bool((plan.options or {}).get("streaming_compatible", False)),
        )
        source_transform = f"{transform.__module__}.{transform.__name__}"
        transform_module = f"{transform.__module__}.{recipe.transform}Generated"
        traceability = build_compiler_traceability(
            recipe,
            source_transform=source_transform,
            transform_module=transform_module,
        )
        lines = [
            recipe.transform,
            f"  backend: {recipe.backend.name} {recipe.backend.target}",
            "",
            "  streaming:",
            f"    status: {streaming.support.value}",
            f"    required: {str(streaming.required).lower()}",
        ]
        for finding in streaming.findings:
            lines.append(f"    {finding.code}: {finding.support.value} in {finding.step} ({finding.operation})")
        lines.extend(["", "  inputs:"])
        for item in recipe.inputs:
            lines.append(f"    {item.name}: {item.schema.__name__}")
        lines.extend(["", "  steps:"])
        for step in recipe.steps:
            lines.append(f"    {step.name}: {step.input_schema.__name__} -> {step.output_schema.__name__}")
            if step.filters:
                lines.append(f"      filters: {len(step.filters)}")
            if step.joins:
                lines.append(f"      joins: {', '.join(join.input_name for join in step.joins)}")
            hooks = [hook.name for hook in (*step.before_hooks, *step.after_hooks)]
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
        lines.extend(["", f"  output: {recipe.final_validation.schema.__name__}"])
        return "\n".join(lines)


render_explain_report = RenderExplainReport()
