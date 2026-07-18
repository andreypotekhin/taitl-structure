from typing import Protocol


class SchemaAPI(Protocol):
    def materialize(self, schema: object) -> object: ...
