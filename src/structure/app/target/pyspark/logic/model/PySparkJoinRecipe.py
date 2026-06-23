from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.Join import Join
from structure.app.dsl.logic.model.transforms.JoinHint import JoinHint


@dataclass(frozen=True)
class PySparkJoinRecipe:
    input_name: str
    input_schema: type[Structure]
    left_alias: str
    right_alias: str
    how: Join
    hint: JoinHint | None
    predicate: PySparkExpressionRecipe
    occurrence: int
