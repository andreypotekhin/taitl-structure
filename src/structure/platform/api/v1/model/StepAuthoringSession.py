from typing import ContextManager, Protocol

from structure.platform.api.v1.model.SymbolicContext import SymbolicContext


class StepAuthoringSession(ContextManager["StepAuthoringSession"], Protocol):
    def arguments(self) -> tuple[object, ...]: ...

    def context(self) -> SymbolicContext: ...

    def capture(self, value: object) -> object: ...
