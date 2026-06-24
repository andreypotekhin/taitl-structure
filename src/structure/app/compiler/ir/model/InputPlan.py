from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure


@dataclass(frozen=True)
class InputPlan:
    name: str
    schema: type[Structure]
    ordinal: int
