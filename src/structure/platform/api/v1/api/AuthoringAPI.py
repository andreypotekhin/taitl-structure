from typing import Protocol

from structure.platform.api.v1.model import StepAuthoringRequest, StepAuthoringSession


class AuthoringAPI(Protocol):
    def open_step(self, request: StepAuthoringRequest) -> StepAuthoringSession: ...
