from collections.abc import Mapping

from structure.core.dsl.model.types.ArrayType import ArrayType
from structure.core.dsl.model.types.MapType import MapType
from structure.core.dsl.model.types.StructType import StructType
from structure.dsl import FieldDefinition, Schema


class ValidatePySparkSchemas:
    def __call__(self, schema: type[Schema]) -> None:
        self.validate(schema, schema._structure_fields)

    def validate(self, schema: type, fields: Mapping[str, FieldDefinition]) -> None:
        columns: dict[str, str] = {}
        for field in fields.values():
            other = columns.get(field.column)
            if other is not None:
                raise ValueError(f"{schema.__name__} has duplicate Spark column name {field.column!r}. Use a unique field alias.")
            columns[field.column] = field.name
            self._acyclic(schema, field.type, (schema,))

    def _acyclic(self, root: type[Schema], type: object, path: tuple[type[Schema], ...]) -> None:
        if isinstance(type, ArrayType):
            self._acyclic(root, type.element, path)
        elif isinstance(type, MapType):
            self._acyclic(root, type.key, path)
            self._acyclic(root, type.value, path)
        elif isinstance(type, StructType):
            if type.schema in path:
                cycle = " -> ".join(schema.__name__ for schema in (*path, type.schema))
                raise ValueError(f"{root.__name__} has recursive Struct(...) schema composition: {cycle}. Nested Schema fields must form an acyclic schema graph.")
            for field in type.schema._structure_fields.values():
                self._acyclic(root, field.type, (*path, type.schema))
