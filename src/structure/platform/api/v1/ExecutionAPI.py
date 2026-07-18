from typing import Protocol


class ExecutionAPI(Protocol):
    def execute(self, payload: object, runtime: object) -> object: ...
