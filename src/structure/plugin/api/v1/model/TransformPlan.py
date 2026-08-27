from dataclasses import dataclass
from typing import Any

from structure.plugin.api.v1.model.InputPlan import InputPlan
from structure.plugin.api.v1.model.OutputPlan import OutputPlan
from structure.plugin.api.v1.model.StageOutputPlan import StageOutputPlan
from structure.plugin.api.v1.model.StepPlan import StepPlan
from structure.plugin.api.v1.model.StreamingBoundaryPlan import StreamingBoundaryPlan


@dataclass(frozen=True)
class TransformPlan:
    """Core-owned transform structure routed to a selected plugin compiler."""

    name: str
    inputs: tuple[InputPlan, ...]
    steps: tuple[StepPlan, ...]
    outputs: tuple[OutputPlan, ...]
    stage_outputs: tuple[StageOutputPlan, ...] = ()
    allow_stage_outputs: bool = True
    internal_inputs: tuple[InputPlan, ...] = ()
    options: dict[str, object] | None = None
    diagnostics: tuple[Any, ...] = ()
    streaming_boundaries: tuple[StreamingBoundaryPlan, ...] = ()

    @property
    def output_schema(self) -> Any:
        if len(self.outputs) != 1:
            names = ", ".join(output.name for output in self.outputs)
            raise ValueError(f"Transform has multiple outputs: {names}. Use TransformPlan.outputs instead.")
        return self.outputs[0].schema
