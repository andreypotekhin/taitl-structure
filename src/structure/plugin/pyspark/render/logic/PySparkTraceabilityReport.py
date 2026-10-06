import json
from collections.abc import Mapping

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class PySparkTraceabilityReport:

    def __init__(self, schema_names: Mapping[type[Schema], str] | None = None) -> None:
        from structure.plugin.pyspark.api.PySpark import PySpark

        self._schema = PySpark.schema.render(schema_names)

    @property
    def _traceability(self):
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark.compiler.traceability()

    def render(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        transform_module: str,
        schema_modules: Mapping[type[Schema], str],
    ) -> str:
        traceability = self._traceability(
            plan,
            source_transform=source_transform,
            transform_module=transform_module,
        )
        data: dict[str, object] = {
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

    def _schema_constants(self, schema_modules: Mapping[type[Schema], str]) -> dict[str, dict[str, str]]:
        return {
            f"{schema.__module__}.{schema.__qualname__}": {
                "constant": self._schema.constant_name(schema),
                "module": module,
            }
            for schema, module in sorted(schema_modules.items(), key=lambda item: item[0].__name__)
        }

    def _step(self, step: PySparkStepRecipe) -> dict[str, object]:
        stateful = [
            operation.stateful_transform
            for operation in step.operations
            if operation.stateful_transform is not None
        ]
        legacy_stateful = [
            operation.legacy_pandas_state
            for operation in step.operations
            if operation.legacy_pandas_state is not None
        ]
        data: dict[str, object] = {
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
        if stateful or legacy_stateful:
            data["stateful_operations"] = [
                {
                    "input_schema": state.input_schema.__name__,
                    "key": (
                        [
                            (key.data or {}).get("name", (key.data or {}).get("field", "expression"))
                            for key in state.key
                        ]
                        if isinstance(state.key, tuple)
                        else (state.key.data or {}).get("name", (state.key.data or {}).get("field", "expression"))
                    ),
                    "key_schema": state.key_schema.__name__,
                    "mode": state.processor_mode,
                    "operation": (
                        "transform_with_state_in_pandas" if state.interface == "pandas" else "transform_with_state"
                    ),
                    "output_mode": state.output_mode,
                    "output_schema": state.output_schema.__name__,
                    "state_schemas": [schema.__name__ for schema in state.state_schemas],
                    "time_mode": state.time_mode,
                }
                for state in stateful
            ] + [
                {
                    "input_schema": state.input_schema.__name__,
                    "key": (state.key.data or {}).get("name", (state.key.data or {}).get("field", "expression")),
                    "key_schema": state.key_schema.__name__,
                    "mode": state.processor_mode,
                    "operation": "apply_in_pandas_with_state",
                    "output_mode": state.output_mode,
                    "output_schema": state.output_schema.__name__,
                    "state_schemas": [state.state_schema.__name__],
                    "timeout": state.timeout,
                }
                for state in legacy_stateful
            ]
        budgets = [
            {
                "id": f"{step.ordinal}:{index}",
                "operation": operation.kind,
                "max_rows": operation.state_budget.max_rows,
                "max_state_bytes": operation.state_budget.max_state_bytes,
            }
            for index, operation in enumerate(step.operations)
            if operation.state_budget is not None
        ]
        if budgets:
            data["state_budgets"] = budgets
        return data

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
