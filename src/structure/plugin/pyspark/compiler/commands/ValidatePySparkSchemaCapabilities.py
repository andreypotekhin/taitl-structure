from typing import cast

from structure.dsl import Schema
from structure.plugin.api.v1.model import BackendCapabilities, CapabilityRequirement, TransformPlan
from structure.plugin.pyspark.dsl.types import ArrayType, MapType, StructType, StructureType, VariantType


class ValidatePySparkSchemaCapabilities:
    """Require target capabilities for the types used by a transform's schemas."""

    def __call__(self, plan: TransformPlan, *, capabilities: BackendCapabilities) -> None:
        for schema in self._schemas(plan):
            self._schema(schema, capabilities=capabilities, visited=set())

    def _schemas(self, plan: TransformPlan) -> set[type[Schema]]:
        schemas = {cast(type[Schema], input.schema) for input in plan.inputs}
        schemas.update(cast(type[Schema], step.input_schema) for step in plan.steps)
        schemas.update(cast(type[Schema], step.output_schema) for step in plan.steps)
        schemas.update(cast(type[Schema], result.schema) for step in plan.steps for result in step.results)
        schemas.update(cast(type[Schema], output.schema) for output in plan.outputs)
        return schemas

    def _schema(self, schema: type[Schema], *, capabilities: BackendCapabilities, visited: set[type[Schema]]) -> None:
        if schema in visited:
            return
        visited.add(schema)
        for field in schema._structure_fields.values():
            self._type(field.type, capabilities=capabilities, visited=visited)

    def _type(self, type: StructureType, *, capabilities: BackendCapabilities, visited: set[type[Schema]]) -> None:
        if isinstance(type, VariantType):
            capabilities.require(CapabilityRequirement(group="schema", name="variant"))
        elif isinstance(type, ArrayType):
            self._type(type.element, capabilities=capabilities, visited=visited)
        elif isinstance(type, MapType):
            self._type(type.key, capabilities=capabilities, visited=visited)
            self._type(type.value, capabilities=capabilities, visited=visited)
        elif isinstance(type, StructType):
            self._schema(type.schema, capabilities=capabilities, visited=visited)
