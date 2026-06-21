from __future__ import annotations

import re

from structure.app.dsl.logic.model.schemas.FieldDefinition import FieldDefinition
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.types.ArrayType import ArrayType
from structure.app.dsl.logic.model.types.BooleanType import BooleanType
from structure.app.dsl.logic.model.types.DateType import DateType
from structure.app.dsl.logic.model.types.DecimalType import DecimalType
from structure.app.dsl.logic.model.types.DoubleType import DoubleType
from structure.app.dsl.logic.model.types.FloatType import FloatType
from structure.app.dsl.logic.model.types.IntegerType import IntegerType
from structure.app.dsl.logic.model.types.LongType import LongType
from structure.app.dsl.logic.model.types.MapType import MapType
from structure.app.dsl.logic.model.types.StringType import StringType
from structure.app.dsl.logic.model.types.StructType import StructType
from structure.app.dsl.logic.model.types.StructureType import StructureType
from structure.app.dsl.logic.model.types.TimestampType import TimestampType


class RenderPySparkSchema:

    def constant_name(self, schema: type[Structure]) -> str:
        return f"{self._upper_snake(schema.__name__)}_SCHEMA"

    def __call__(self, schema: type[Structure]) -> str:
        return f"{self.constant_name(schema)} = {self.expression(schema)}"

    def expression(self, schema: type[Structure]) -> str:
        bases = schema._structure_schema_bases
        local = tuple(schema._structure_local_fields.values())
        if bases and local:
            fields = self._fields(local)
            base_fields = " + ".join(f"{self.constant_name(base)}.fields" for base in bases)
            return f"T.StructType({base_fields} + [\n{fields}\n])"
        if bases:
            base_fields = " + ".join(f"{self.constant_name(base)}.fields" for base in bases)
            return f"T.StructType({base_fields})"

        fields = self._fields(schema._structure_fields.values())
        return f"T.StructType([\n{fields}\n])"

    def field(self, field: FieldDefinition) -> str:
        nullable = "True" if field.nullable else "False"
        return f'    T.StructField("{field.name}", {self.type(field.type)}, {nullable}),'

    def type(self, type: StructureType) -> str:
        if isinstance(type, StringType):
            return "T.StringType()"
        if isinstance(type, IntegerType):
            return "T.IntegerType()"
        if isinstance(type, LongType):
            return "T.LongType()"
        if isinstance(type, FloatType):
            return "T.FloatType()"
        if isinstance(type, DoubleType):
            return "T.DoubleType()"
        if isinstance(type, BooleanType):
            return "T.BooleanType()"
        if isinstance(type, DateType):
            return "T.DateType()"
        if isinstance(type, TimestampType):
            return "T.TimestampType()"
        if isinstance(type, DecimalType):
            return f"T.DecimalType({type.precision}, {type.scale})"
        if isinstance(type, ArrayType):
            contains_null = "True" if type.contains_null else "False"
            return f"T.ArrayType({self.type(type.element)}, containsNull={contains_null})"
        if isinstance(type, MapType):
            value_contains_null = "True" if type.value_contains_null else "False"
            return f"T.MapType({self.type(type.key)}, {self.type(type.value)}, valueContainsNull={value_contains_null})"
        if isinstance(type, StructType):
            return self.constant_name(type.schema)
        raise TypeError(f"Unsupported Structure type: {type!r}")

    def _fields(self, fields) -> str:
        return "\n".join(self.field(field) for field in fields)

    def _upper_snake(self, value: str) -> str:
        return re.sub(r"(?<!^)(?=[A-Z])", "_", value).upper()


render_pyspark_schema = RenderPySparkSchema()
