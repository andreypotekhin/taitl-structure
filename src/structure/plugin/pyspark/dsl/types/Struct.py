from __future__ import annotations

from typing import TYPE_CHECKING

from structure.plugin.pyspark.dsl.types.StructType import StructType

if TYPE_CHECKING:
    from structure.dsl import Schema


class Struct(StructType):
    def __init__(self, schema: type[Schema]) -> None:
        super().__init__(schema)
