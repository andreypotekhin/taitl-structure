from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class PosexplodeStructPlan:
    expression: Expression
    scope: str
    schema: type[Schema]
    ordinal: str | None
    function: str = "posexplode"
    outer: bool = False
