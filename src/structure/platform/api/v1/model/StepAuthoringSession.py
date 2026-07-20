from typing import ContextManager, Protocol

from structure.platform.api.v1.model.StepAuthoringCapture import StepAuthoringCapture


class StepAuthoringSession(ContextManager["StepAuthoringSession"], Protocol):
    def arguments(self) -> tuple[object, ...]: ...

    def capture(self, value: object) -> StepAuthoringCapture: ...
