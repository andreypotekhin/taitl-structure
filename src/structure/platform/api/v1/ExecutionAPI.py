from typing import Protocol

from structure.platform.api.v1.ExecutionRequest import ExecutionRequest


class ExecutionAPI(Protocol):
    def execute(self, request: ExecutionRequest) -> object: ...
