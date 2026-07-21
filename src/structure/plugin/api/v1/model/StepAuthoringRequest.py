from dataclasses import dataclass
from typing import Mapping

from structure.plugin.api.v1.model.StepAuthoringInput import StepAuthoringInput
from structure.plugin.api.v1.model.StepAuthoringResult import StepAuthoringResult


@dataclass(frozen=True)
class StepAuthoringRequest:
    target: str
    configuration: Mapping[str, object]
    name: str
    origin: object | None
    inputs: tuple[StepAuthoringInput, ...]
    results: tuple[StepAuthoringResult, ...]
    options: Mapping[str, object] | None = None
    capture_special_exprs: bool = False
