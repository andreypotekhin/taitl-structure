from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class StepInputPlan:
    parameter: str
    schema: type[Structure]
    source: str
    scope: str
    lane: str
    ordinal: int
    driving: bool
