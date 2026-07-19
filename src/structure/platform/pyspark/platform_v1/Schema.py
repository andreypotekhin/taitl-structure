from typing import Any, cast

from structure.core.runtime.schemas.model.TransformSchemas import TransformSchemas
from structure.platform.api.v1 import SchemaAPI, SchemaInspectionRequest, TransformSchemaRequest
from structure.platform.pyspark.api import PySpark
from structure.platform.pyspark.logic.MapPySparkSchemaToStructureSource import MapPySparkSchemaToStructureSource
from structure.platform.pyspark.schemas.BuildTransformSchemas import BuildTransformSchemas
from structure.platform.pyspark.schemas.ReadPySparkSchema import ReadPySparkSchema


class Schema(SchemaAPI):
    def materialize(self, schema: object) -> object:
        return PySpark.schema.materialize()(cast(Any, schema))

    def build(self, request: TransformSchemaRequest) -> TransformSchemas:
        return BuildTransformSchemas()(cast(Any, request.payload), types=request.types)

    def read(self, request: SchemaInspectionRequest) -> object:
        return ReadPySparkSchema()(request)

    def source(self, schema: object, *, to: str) -> object:
        return MapPySparkSchemaToStructureSource()(schema, to=to)
