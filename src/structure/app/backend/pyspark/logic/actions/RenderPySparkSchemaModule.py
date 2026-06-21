from __future__ import annotations

from collections import defaultdict
from typing import Mapping, Sequence

from structure.app.backend.pyspark.logic.actions.RenderPySparkSchema import render_pyspark_schema
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.types.ArrayType import ArrayType
from structure.app.dsl.logic.model.types.MapType import MapType
from structure.app.dsl.logic.model.types.StructType import StructType
from structure.app.dsl.logic.model.types.StructureType import StructureType


class RenderPySparkSchemaModule:

    def __call__(
        self,
        schemas: Sequence[type[Structure]],
        *,
        dependency_modules: Mapping[type[Structure], str] | None = None,
    ) -> str:
        dependencies = self._dependencies(schemas, dependency_modules or {})
        imports = self._imports(dependencies)
        constants = "\n\n".join(render_pyspark_schema(schema) for schema in schemas)
        return f"{imports}\n\n\n{constants}\n"

    def _imports(self, dependencies: Mapping[str, tuple[str, ...]]) -> str:
        lines = ["from pyspark.sql import types as T"]
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
        schemas: Sequence[type[Structure]],
        dependency_modules: Mapping[type[Structure], str],
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
                modules[module].add(render_pyspark_schema.constant_name(dependency))

        return {module: tuple(sorted(constants)) for module, constants in modules.items()}

    def _schema_dependencies(self, schema: type[Structure]) -> set[type[Structure]]:
        dependencies: set[type[Structure]] = set(schema._structure_schema_bases)
        for field in schema._structure_fields.values():
            dependencies.update(self._type_dependencies(field.type))
        return dependencies

    def _type_dependencies(self, type: StructureType) -> set[type[Structure]]:
        if isinstance(type, StructType):
            return {type.schema}
        if isinstance(type, ArrayType):
            return self._type_dependencies(type.element)
        if isinstance(type, MapType):
            return self._type_dependencies(type.key) | self._type_dependencies(type.value)
        return set()


render_pyspark_schema_module = RenderPySparkSchemaModule()
