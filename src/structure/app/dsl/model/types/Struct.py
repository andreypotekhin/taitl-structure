from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.types.StructType import StructType


class Struct(StructType):

    def __init__(self, schema: type[Schema]) -> None:
        if not isinstance(schema, type) or not issubclass(schema, Schema):
            raise TypeError("Struct(...) requires a Schema class")
        super().__init__(schema)
