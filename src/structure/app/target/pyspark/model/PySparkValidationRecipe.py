from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode


@dataclass(frozen=True)
class PySparkValidationRecipe:
    target: str
    schema: type[Structure]
    mode: SchemaMode
    project: bool
    reason: str
