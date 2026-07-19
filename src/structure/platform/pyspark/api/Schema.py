from typing import Any, cast

from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.platform.api.v1 import (
    SchemaAPI,
    SchemaInspectionRequest,
    SchemaValidationRequest,
    TransformSchemaRequest,
)
from structure.platform.pyspark.api.PySpark import PySpark
from structure.platform.pyspark.dsl.ValidatePySparkSchemas import ValidatePySparkSchemas


class Schema(SchemaAPI):
    def __init__(self) -> None:
        self._validate = ValidatePySparkSchemas()

    def validate(self, request: SchemaValidationRequest) -> None:
        from structure.dsl import Schema as StructureSchema

        for schema in request.schemas:
            if not isinstance(schema, type) or not issubclass(schema, StructureSchema):
                raise TypeError("PLATFORM-E2708: PySpark schema validation requires Structure Schema classes.")
            self._validate(schema)

    def materialize(self, schema: object) -> object:
        return PySpark.schema.materialize()(cast(Any, schema))

    def build(self, request: TransformSchemaRequest) -> TransformSchemas:
        return PySpark.schema.build()(cast(Any, request.payload), types=request.types)

    def read(self, request: SchemaInspectionRequest) -> object:
        return PySpark.schema.read()(request)

    def source(self, schema: object, *, to: str) -> object:
        return PySpark.schema.source()(schema, to=to)
