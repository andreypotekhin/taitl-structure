from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class MapGeneratorPlan:
    expression: Expression
    scope: str
    schema: type[Schema]
    key_field: str
    value_field: str
    ordinal: str | None
    function: str = "posexplode"
    outer: bool = False
