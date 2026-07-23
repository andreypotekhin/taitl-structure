from typing import Protocol

from structure.plugin.api.v1.model import StepAuthoringRequest, StepAuthoringResult, StepAuthoringSession


class AuthoringAPI(Protocol):
    def open_step(self, request: StepAuthoringRequest) -> StepAuthoringSession: ...

    def result_arguments(self, results: tuple[StepAuthoringResult, ...]) -> tuple[object, ...]: ...
