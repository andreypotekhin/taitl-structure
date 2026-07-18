from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.SchemaMode import SchemaMode


@dataclass(frozen=True)
class PySparkValidationRecipe:
    target: str
    schema: type[Schema]
    mode: SchemaMode
    project: bool
    reason: str
