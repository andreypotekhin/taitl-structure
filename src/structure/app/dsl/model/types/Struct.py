from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.types.StructType import StructType


class Struct(StructType):

    def __init__(self, schema: type[Structure]) -> None:
        if not isinstance(schema, type) or not issubclass(schema, Structure):
            raise TypeError("Struct(...) requires a Structure schema class")
        super().__init__(schema)
