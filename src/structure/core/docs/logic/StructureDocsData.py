from __future__ import annotations

from collections.abc import Mapping

from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.compiler.ir.model.StepPlan import StepPlan
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.dsl.model.schemas.FieldDefinition import FieldDefinition
from structure.core.dsl.model.schemas.Schema import Schema
from structure.plugin.pyspark.dsl.types import ArrayType, DecimalType, MapType, StructType, StructureType


class StructureDocsData:

    def project(
        self,
        project: DiscoveredStructureProject,
        plans: Mapping[str, TransformPlan],
        *,
        platform_details: Mapping[str, Mapping[str, object]],
    ) -> dict[str, object]:
        schemas = [self.schema(schema, module) for module, items in project.schema_modules.items() for schema in items]
        transforms = [
            self.transform(source, plan, platform_details.get(source, {})) for source, plan in sorted(plans.items())
        ]
        return {
            "schemas": sorted(schemas, key=lambda item: str(item["name"])),
            "transforms": transforms,
        }

    def schema(self, schema: type[Schema], module: str) -> dict[str, object]:
        return {
            "name": schema.__name__,
            "module": module,
            "bases": [base.__name__ for base in schema._structure_schema_bases],
            "fields": [self.field(field) for field in schema._structure_fields.values()],
        }

    def field(self, field: FieldDefinition) -> dict[str, object]:
        data: dict[str, object] = {
            "name": field.name,
            "column": field.column,
            "type": self.type(field.type),
            "nullable": field.nullable,
        }
        if field.description:
            data["description"] = field.description
        if field.metadata:
            data["metadata"] = dict(field.metadata)
        return data

    def transform(self, source: str, plan: TransformPlan, platform_details: Mapping[str, object]) -> dict[str, object]:
        return {
            "name": plan.name,
            "source": source,
            "inputs": [
                {"name": item.name, "schema": getattr(item.schema, "__name__", str(item.schema)), "ordinal": item.ordinal}
                for item in plan.inputs
            ],
            "outputs": [
                {"name": item.name, "schema": item.schema.__name__, "ordinal": item.ordinal} for item in plan.outputs
            ],
            "step_methods": [self.step(step, self._step_details(platform_details, step.name)) for step in plan.steps],
            "dependencies": sorted(self._dependencies(plan, platform_details)),
            "target_artifacts": {
                "pyspark_transform": self._target_transform(source),
                "traceability": self._traceability(source, plan),
            },
        }

    def step(self, step: StepPlan, platform_details: Mapping[str, object]) -> dict[str, object]:
        data: dict[str, object] = {
            "name": step.name,
            "input_lane": step.input_lane,
            "input_schema": step.input_schema.__name__,
            "output_lane": step.output_lane,
            "output_schema": step.output_schema.__name__,
            "inputs": [
                {
                    "parameter": item.parameter,
                    "source": item.source,
                    "schema": getattr(item.schema, "__name__", str(item.schema)),
                    "driving": item.driving,
                }
                for item in step.inputs
            ],
            "results": [
                {"lane": item.lane, "schema": item.schema.__name__, "frame": item.frame} for item in step.results
            ],
        }
        if joins := platform_details.get("joins"):
            data["joins"] = joins
        if step.before_hooks:
            data["before_hooks"] = [hook.name for hook in step.before_hooks]
        if step.after_hooks:
            data["after_hooks"] = [hook.name for hook in step.after_hooks]
        return data

    def type(self, item: StructureType) -> str:
        if isinstance(item, DecimalType):
            return f"decimal({item.precision},{item.scale})"
        if isinstance(item, ArrayType):
            nulls = "?" if item.contains_null else "!"
            return f"array<{self.type(item.element)}{nulls}>"
        if isinstance(item, MapType):
            nulls = "?" if item.value_contains_null else "!"
            return f"map<{self.type(item.key)},{self.type(item.value)}{nulls}>"
        if isinstance(item, StructType):
            return item.schema.__name__
        return item.name

    def _dependencies(self, plan: TransformPlan, platform_details: Mapping[str, object]) -> set[str]:
        dependencies: set[str] = set()
        for step in plan.steps:
            dependencies.update(item.source for item in step.inputs if not item.driving)
        target_dependencies = platform_details.get("dependencies", ())
        if isinstance(target_dependencies, (tuple, list, set)):
            dependencies.update(str(item) for item in target_dependencies)
        return dependencies

    @staticmethod
    def _step_details(platform_details: Mapping[str, object], name: str) -> Mapping[str, object]:
        steps = platform_details.get("steps", {})
        return steps.get(name, {}) if isinstance(steps, Mapping) else {}

    def _target_transform(self, source: str) -> str:
        module = source.rsplit(".", 2)[1]
        return f"pyspark/transforms/{module}.py"

    def _traceability(self, source: str, plan: TransformPlan) -> str:
        module = source.rsplit(".", 2)[1]
        return f"traceability/transforms/{module}.{plan.name}.json"
