from __future__ import annotations

from structure.platform.api.v1.model.TransformPlan import TransformPlan
from structure.platform.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class DescribePySparkDocumentation:
    """Extract the bundled PySpark details displayed by Structure documentation."""

    def __call__(self, plan: TransformPlan) -> dict[str, object]:
        steps: dict[str, dict[str, object]] = {}
        dependencies: set[str] = set()
        for step in plan.steps:
            body = step.platform_body
            if not isinstance(body, PySparkStepBody):
                continue
            joins = tuple({"input": join.input_name, "how": join.how.value} for join in body.joins)
            if joins:
                steps[step.name] = {"joins": joins}
                dependencies.update(join["input"] for join in joins)
        return {"steps": steps, "dependencies": tuple(sorted(dependencies))}
