from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class ScalarGeneratorPlan:
    expression: Expression
    scope: str
    schema: type[Schema]
    value_field: str
    ordinal: str | None
    function: str = "posexplode"
    outer: bool = False
