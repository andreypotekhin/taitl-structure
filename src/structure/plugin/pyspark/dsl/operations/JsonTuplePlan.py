from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class JsonTuplePlan:
    """Immutable plan for Spark's row-preserving multi-column JSON extractor."""

    expression: Expression
    scope: str
    schema: type[Schema]
    fields: tuple[tuple[str, str], ...]
