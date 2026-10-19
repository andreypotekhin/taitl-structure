from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkRelationQueryRecipe:
    """Compiler-owned representation of an immutable typed relation query."""

    source_name: str
    source: str
    source_schema: type[Schema]
    output_schema: type[Schema]
    output_scope: str
    operations: tuple[tuple[str, object], ...]

    @property
    def bound(self) -> int | None:
        value = next((value for kind, value in reversed(self.operations) if kind == "limit"), None)
        return None if value is None else cast(int, value)
