from typing import Protocol

from structure.platform.api.v1.SchemaInspectionRequest import SchemaInspectionRequest
from structure.platform.api.v1.SchemaValidationRequest import SchemaValidationRequest
from structure.platform.api.v1.TransformSchemaRequest import TransformSchemaRequest


class SchemaAPI(Protocol):
    def validate(self, request: SchemaValidationRequest) -> None: ...

    def materialize(self, schema: object) -> object: ...

    def build(self, request: TransformSchemaRequest) -> object: ...

    def read(self, request: SchemaInspectionRequest) -> object: ...

    def source(self, schema: object, *, to: str) -> object: ...
