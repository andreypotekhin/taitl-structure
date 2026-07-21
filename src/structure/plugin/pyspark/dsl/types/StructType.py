from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.plugin.pyspark.dsl.types.StructureType import StructureType

if TYPE_CHECKING:
    from structure.dsl import Schema


@dataclass(frozen=True)
class StructType(StructureType):
    schema: type[Schema]

    def __init__(self, schema: type[Schema]) -> None:
        from structure.dsl import Schema

        if not isinstance(schema, type) or not issubclass(schema, Schema):
            raise TypeError("Struct(...) requires a Schema class")
        object.__setattr__(self, "name", "struct")
        object.__setattr__(self, "schema", schema)
