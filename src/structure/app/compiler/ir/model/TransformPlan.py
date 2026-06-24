from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.InputPlan import InputPlan
from structure.app.compiler.ir.model.OutputPlan import OutputPlan
from structure.app.compiler.ir.model.StepPlan import StepPlan
from structure.app.dsl.model.schemas.Structure import Structure
from structure.lib.cross.errors import Diagnostic


@dataclass(frozen=True)
class TransformPlan:
    name: str
    inputs: tuple[InputPlan, ...]
    steps: tuple[StepPlan, ...]
    outputs: tuple[OutputPlan, ...]
    options: dict[str, object] | None = None
    diagnostics: tuple[Diagnostic, ...] = ()

    @property
    def output_schema(self) -> type[Structure]:
        if len(self.outputs) != 1:
            names = ", ".join(output.name for output in self.outputs)
            raise ValueError(f"Transform has multiple outputs: {names}. Use TransformPlan.outputs instead.")
        return self.outputs[0].schema
