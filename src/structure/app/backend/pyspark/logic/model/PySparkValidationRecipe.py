from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.SchemaMode import SchemaMode


@dataclass(frozen=True)
class PySparkValidationRecipe:
    target: str
    schema: type[Structure]
    mode: SchemaMode
    project: bool
    reason: str
