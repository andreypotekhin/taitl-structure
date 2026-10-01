from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkJsonTupleRecipe:
    """Mapped recipe for a typed ``json_tuple`` generated scope."""

    expression: PySparkExpressionRecipe
    scope: str
    schema: type[Schema]
    fields: tuple[tuple[str, str], ...]
