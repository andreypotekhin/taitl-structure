import json
from collections.abc import Mapping

from structure.app.compiler.api import Compiler
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.target.pyspark.commands.RenderPySparkSchema import render_pyspark_schema
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.model.PySparkStepRecipe import PySparkStepRecipe


class PySparkTraceabilityReport:

    def render(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        transform_module: str,
        schema_modules: Mapping[type[Structure], str],
    ) -> str:
        traceability = Compiler.traceability.build()(
            plan,
            source_transform=source_transform,
            transform_module=transform_module,
        )
        data = {
            "backend": {"name": plan.backend.name, "target": plan.backend.target},
            "generated_transform_class": f"{plan.transform}Generated",
            "inputs": [
                {"name": item.name, "ordinal": item.ordinal, "schema": item.schema.__name__} for item in plan.inputs
            ],
            "provenance": [record.to_dict() for record in traceability.provenance],
            "schema_constants": self._schema_constants(schema_modules),
            "source_transform": source_transform,
            "static_dataflow": {
                "dependencies": [dependency.to_dict() for dependency in traceability.static_dataflow],
                "opaque_boundaries": [boundary.to_dict() for boundary in traceability.opaque_boundaries],
            },
            "steps": [self._step(step) for step in plan.steps],
            "target_module": transform_module,
            "validation": {
                "outputs": [
                    {
                        "name": output.name,
                        "mode": output.validation.mode.value,
                        "schema": output.validation.schema.__name__,
                    }
                    for output in plan.outputs
                ]
            },
        }
        return json.dumps(data, indent=2, sort_keys=True) + "\n"

    def _schema_constants(self, schema_modules: Mapping[type[Structure], str]) -> dict[str, dict[str, str]]:
        return {
            schema.__name__: {
                "constant": render_pyspark_schema.constant_name(schema),
                "module": module,
            }
            for schema, module in sorted(schema_modules.items(), key=lambda item: item[0].__name__)
        }

    def _step(self, step: PySparkStepRecipe) -> dict[str, object]:
        return {
            "after_hooks": [hook.name for hook in step.after_hooks],
            "before_hooks": [hook.name for hook in step.before_hooks],
            "input_alias": step.input_alias,
            "joins": [self._join(join) for join in step.joins],
            "name": step.name,
            "output_alias": step.output_alias,
            "output_schema": step.output_schema.__name__,
            "results": [
                {
                    "after_hooks": [hook.name for hook in result.after_hooks],
                    "frame": result.frame,
                    "lane": result.lane,
                    "schema": result.schema.__name__,
                }
                for result in step.results
            ],
            "validation": [
                {
                    "mode": validation.mode.value,
                    "project": validation.project,
                    "reason": validation.reason,
                    "schema": validation.schema.__name__,
                }
                for validation in step.validations
            ],
        }

    def _join(self, join) -> dict[str, str]:
        data = {
            "how": join.how.value,
            "input": join.input_name,
            "right_alias": join.right_alias,
        }
        if join.strategy is not None:
            data["strategy"] = join.strategy.value
        if join.dedupe is not None:
            data["dedupe"] = join.dedupe.direction
            data["ties"] = join.dedupe.ties.value
        return data
