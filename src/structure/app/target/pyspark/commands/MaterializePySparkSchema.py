from __future__ import annotations

from structure.app.dsl.model.schemas.FieldDefinition import FieldDefinition
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DateType import DateType
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.FloatType import FloatType
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.dsl.model.types.TimestampType import TimestampType


class MaterializePySparkSchema:

    def __call__(self, schema: type[Structure], *, types=None):
        spark_types = types or self._spark_types()
        return spark_types.StructType(
            [self.field(field, types=spark_types) for field in schema._structure_fields.values()]
        )

    def field(self, field: FieldDefinition, *, types=None):
        spark_types = types or self._spark_types()
        return spark_types.StructField(field.column, self.type(field.type, types=spark_types), field.nullable)

    def type(self, type: StructureType, *, types=None):
        spark_types = types or self._spark_types()
        if isinstance(type, StringType):
            return spark_types.StringType()
        if isinstance(type, IntegerType):
            return spark_types.IntegerType()
        if isinstance(type, LongType):
            return spark_types.LongType()
        if isinstance(type, FloatType):
            return spark_types.FloatType()
        if isinstance(type, DoubleType):
            return spark_types.DoubleType()
        if isinstance(type, BooleanType):
            return spark_types.BooleanType()
        if isinstance(type, DateType):
            return spark_types.DateType()
        if isinstance(type, TimestampType):
            return spark_types.TimestampType()
        if isinstance(type, DecimalType):
            return spark_types.DecimalType(type.precision, type.scale)
        if isinstance(type, ArrayType):
            return spark_types.ArrayType(
                self.type(type.element, types=spark_types),
                containsNull=type.contains_null,
            )
        if isinstance(type, MapType):
            return spark_types.MapType(
                self.type(type.key, types=spark_types),
                self.type(type.value, types=spark_types),
                valueContainsNull=type.value_contains_null,
            )
        if isinstance(type, StructType):
            return self(type.schema, types=spark_types)
        raise TypeError(f"Unsupported Structure type: {type!r}")

    def _spark_types(self):
        from pyspark.sql import types  # type: ignore[import-not-found]

        return types


materialize_pyspark_schema = MaterializePySparkSchema()
