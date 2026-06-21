from __future__ import annotations

from structure.app.backend.pyspark.api import lower_pyspark_plan
from structure.app.dsl.api import compile_transform
from structure.app.dsl.logic.model.transforms.Transform import Transform


class RenderExplainReport:

    def __call__(self, transform: type[Transform]) -> str:
        recipe = lower_pyspark_plan(compile_transform(transform))
        lines = [
            recipe.transform,
            f"  backend: {recipe.backend.name} {recipe.backend.target}",
            "",
            "  inputs:",
        ]
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
        lines.extend(["", f"  output: {recipe.final_validation.schema.__name__}"])
        return "\n".join(lines)


render_explain_report = RenderExplainReport()
