from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.plans.InputPlan import InputPlan
from structure.app.dsl.logic.model.plans.StepPlan import StepPlan
from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class TransformPlan:
    name: str
    inputs: tuple[InputPlan, ...]
    steps: tuple[StepPlan, ...]
    options: dict[str, object] | None = None

    @property
    def output_schema(self) -> type[Structure]:
        return self.steps[-1].output_schema
