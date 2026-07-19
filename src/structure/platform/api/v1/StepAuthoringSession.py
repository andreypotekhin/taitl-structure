from typing import ContextManager, Protocol


class StepAuthoringSession(ContextManager["StepAuthoringSession"], Protocol):
    def arguments(self) -> tuple[object, ...]: ...

    def capture(self, value: object) -> object: ...
