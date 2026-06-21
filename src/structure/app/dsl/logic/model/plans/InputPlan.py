from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class InputPlan:
    name: str
    schema: type[Structure]
    ordinal: int
