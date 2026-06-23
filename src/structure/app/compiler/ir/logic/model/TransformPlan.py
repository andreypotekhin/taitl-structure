from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.logic.model.InputPlan import InputPlan
from structure.app.compiler.ir.logic.model.OutputPlan import OutputPlan
from structure.app.compiler.ir.logic.model.StepPlan import StepPlan
from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class TransformPlan:
    name: str
    inputs: tuple[InputPlan, ...]
    steps: tuple[StepPlan, ...]
    outputs: tuple[OutputPlan, ...]
    options: dict[str, object] | None = None

    @property
    def output_schema(self) -> type[Structure]:
        if len(self.outputs) != 1:
            names = ", ".join(output.name for output in self.outputs)
            raise ValueError(f"Transform has multiple outputs: {names}. Use TransformPlan.outputs instead.")
        return self.outputs[0].schema
