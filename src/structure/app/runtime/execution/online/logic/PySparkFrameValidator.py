from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.target.pyspark.commands.MaterializePySparkSchema import materialize_pyspark_schema
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


class PySparkFrameValidator:

    def validate(self, df, validation: PySparkValidationRecipe, *, types) -> None:
        schema = materialize_pyspark_schema(validation.schema, types=types)
        actual = df.schema
        actual_names = set(actual.fieldNames())
        expected_names = {field.name for field in schema}
        missing = expected_names - actual_names
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(f"{validation.schema.__name__} is missing required column(s): {names}")
        if validation.mode.value == "strict":
            extra = actual_names - expected_names
            if extra:
                names = ", ".join(sorted(extra))
                raise ValueError(f"{validation.schema.__name__} has unexpected column(s): {names}")
        for expected in schema:
            actual_field = actual[expected.name]
            if actual_field.dataType != expected.dataType:
                raise ValueError(
                    f"{validation.schema.__name__}.{expected.name} expected "
                    f"{expected.dataType}, got {actual_field.dataType}"
                )

    def project(self, df, validation: PySparkValidationRecipe, *, types, functions):
        schema = materialize_pyspark_schema(validation.schema, types=types)
        return df.select(*(functions.col(field.name) for field in schema))

    def cast(self, column, field, expression, *, types):
        if not self._needs_cast(field.type, expression):
            return column
        return column.cast(materialize_pyspark_schema.type(field.type, types=types))

    def _needs_cast(self, field_type: StructureType, expression) -> bool:
        if isinstance(field_type, StructType):
            return False
        if expression.type is None:
            return True
        if not self._same_type(expression.type, field_type):
            return True
        return expression.kind == "sub" and isinstance(field_type, DecimalType)

    def _same_type(self, actual: StructureType, target: StructureType) -> bool:
        if actual.name != target.name:
            return False
        if isinstance(actual, DecimalType) and isinstance(target, DecimalType):
            return actual.precision == target.precision and actual.scale == target.scale
        return actual == target or actual.__class__.__name__.removesuffix("Type") == target.__class__.__name__

    def alias(self, column, field, expression):
        if not self._needs_alias(field, expression):
            return column
        return column.alias(field.column)

    def _needs_alias(self, field, expression) -> bool:
        if expression.kind != "field":
            return True
        return expression.data["field"] != field.column
