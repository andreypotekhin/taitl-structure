from structure.plugin.api.v1 import SchemaAPI as SchemaAPIV1
from structure.plugin.api.v1 import SchemaInspectionRequest, SchemaValidationRequest, TransformSchemaRequest


class Schema(SchemaAPIV1):
    """Pass-through schema facet for mapping rows in the starter plugin."""

    def validate(self, request: SchemaValidationRequest) -> None:
        return None

    def materialize(self, schema: object) -> object:
        return schema

    def build(self, request: TransformSchemaRequest) -> object:
        return request.payload

    def read(self, request: SchemaInspectionRequest) -> object:
        return request.schema

    def source(self, schema: object, *, to: str) -> object:
        return schema
