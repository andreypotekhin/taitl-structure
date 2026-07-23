from typing import ContextManager, Protocol

from structure.plugin.api.v1.model.StepAuthoringCapture import StepAuthoringCapture


class StepAuthoringSession(ContextManager["StepAuthoringSession"], Protocol):
    def arguments(self) -> tuple[object, ...]: ...

    def validate(self) -> tuple[object, ...]: ...

    def capture(self, value: object) -> StepAuthoringCapture: ...
