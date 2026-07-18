from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class StepInputPlan:
    parameter: str
    schema: type[Schema]
    source: str
    scope: str
    lane: str
    ordinal: int
    driving: bool
