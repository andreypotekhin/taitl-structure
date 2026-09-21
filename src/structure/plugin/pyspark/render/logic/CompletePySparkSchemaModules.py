from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from typing import cast

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.types import ArrayType, MapType, StructType


class CompletePySparkSchemaModules:
    """Include schema dependencies already known to a compiled PySpark plan."""

    def __call__(
        self, plan: object, modules: Mapping[str, Sequence[type[Schema]]]
    ) -> dict[str, tuple[type[Schema], ...]]:
        result = {module: list(schemas) for module, schemas in modules.items()}
        included = {schema for schemas in modules.values() for schema in schemas}
        visited: set[int] = set()

        def visit(value: object) -> None:
            if id(value) in visited:
                return
            visited.add(id(value))
            if isinstance(value, type):
                if issubclass(value, Schema) and value is not Schema:
                    schema = cast(type[Schema], value)
                    for base in schema.__bases__:
                        visit(base)
                    for field in schema._structure_fields.values():
                        visit(field.type)
                    if schema not in included:
                        result.setdefault(schema.__module__, []).append(schema)
                        included.add(schema)
            elif isinstance(value, StructType):
                visit(value.schema)
            elif isinstance(value, ArrayType):
                visit(value.element)
            elif isinstance(value, MapType):
                visit(value.key)
                visit(value.value)
            elif isinstance(value, Mapping):
                for item in value.values():
                    visit(item)
            elif isinstance(value, (tuple, list)):
                for item in value:
                    visit(item)
            elif is_dataclass(value):
                for field in fields(value):
                    visit(getattr(value, field.name))

        visit(tuple(schema for schemas in modules.values() for schema in schemas))
        visit(plan)
        return {module: tuple(schemas) for module, schemas in result.items()}
