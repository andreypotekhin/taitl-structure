from typing import Protocol

from structure.platform.api.v1.StepAuthoringRequest import StepAuthoringRequest
from structure.platform.api.v1.StepAuthoringSession import StepAuthoringSession


class AuthoringAPI(Protocol):
    def open_step(self, request: StepAuthoringRequest) -> StepAuthoringSession: ...
