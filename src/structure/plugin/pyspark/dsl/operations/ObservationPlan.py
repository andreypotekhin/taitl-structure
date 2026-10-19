from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import ObservationDeclaration, Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class ObservationPlan:
    name: str
    schema: type[Schema]
    metrics: tuple[tuple[str, Expression], ...]
    declaration: ObservationDeclaration
