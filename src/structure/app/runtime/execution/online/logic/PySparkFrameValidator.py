from structure.app.dsl.model.types.StructType import StructType
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
        return df.select(*(functions.col(field.name).cast(field.dataType).alias(field.name) for field in schema))

    def cast(self, column, field, *, types):
        if isinstance(field.type, StructType):
            return column
        return column.cast(materialize_pyspark_schema.type(field.type, types=types))
