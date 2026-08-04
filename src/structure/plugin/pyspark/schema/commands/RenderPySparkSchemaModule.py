from __future__ import annotations

from collections import defaultdict
from typing import Mapping, Sequence

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.types import ArrayType, GeometryType, MapType, StructType, StructureType


class RenderPySparkSchemaModule:

    def __init__(self, schema_names: Mapping[type[Schema], str] | None = None) -> None:
        from structure.plugin.pyspark.schema.commands.RenderPySparkSchema import RenderPySparkSchema

        self._schema = RenderPySparkSchema(schema_names)

    def __call__(
        self,
        schemas: Sequence[type[Schema]],
        *,
        dependency_modules: Mapping[type[Schema], str] | None = None,
    ) -> str:
        dependencies = self._dependencies(schemas, dependency_modules or {})
        imports = self._imports(dependencies, any(self._uses_geometry(schema) for schema in schemas))
        constants = "\n\n".join(self._schema(schema) for schema in schemas)
        return f"{imports}\n\n\n{constants}\n"

    def _imports(self, dependencies: Mapping[str, tuple[str, ...]], geometry: bool) -> str:
        lines = ["from pyspark.sql import types as T"]
        if geometry:
            lines.append("from structure.plugin.pyspark.geo import geometry_type")
        for module in sorted(dependencies):
            constants = dependencies[module]
            if len(constants) == 1:
                lines.append(f"from {module} import {constants[0]}")
            else:
                names = ", ".join(constants)
                lines.append(f"from {module} import {names}")
        return "\n".join(lines)

    def _dependencies(
        self,
        schemas: Sequence[type[Schema]],
        dependency_modules: Mapping[type[Schema], str],
    ) -> Mapping[str, tuple[str, ...]]:
        local = set(schemas)
        modules: dict[str, set[str]] = defaultdict(set)
        for schema in schemas:
            for dependency in self._schema_dependencies(schema):
                if dependency in local:
                    continue
                module = dependency_modules.get(dependency)
                if module is None:
                    continue
                modules[module].add(self._schema.constant_name(dependency))

        return {module: tuple(sorted(constants)) for module, constants in modules.items()}

    def _uses_geometry(self, schema: type[Schema]) -> bool:
        return any(self._type_uses_geometry(field.type) for field in schema._structure_fields.values())

    def _type_uses_geometry(self, type: StructureType) -> bool:
        if isinstance(type, GeometryType):
            return True
        if isinstance(type, ArrayType):
            return self._type_uses_geometry(type.element)
        if isinstance(type, MapType):
            return self._type_uses_geometry(type.key) or self._type_uses_geometry(type.value)
        return False

    def _schema_dependencies(self, schema: type[Schema]) -> set[type[Schema]]:
        dependencies: set[type[Schema]] = set(schema._structure_schema_bases)
        for field in schema._structure_fields.values():
            dependencies.update(self._type_dependencies(field.type))
        return dependencies

    def _type_dependencies(self, type: StructureType) -> set[type[Schema]]:
        if isinstance(type, StructType):
            return {type.schema}
        if isinstance(type, ArrayType):
            return self._type_dependencies(type.element)
        if isinstance(type, MapType):
            return self._type_dependencies(type.key) | self._type_dependencies(type.value)
        return set()


render_pyspark_schema_module = RenderPySparkSchemaModule()
