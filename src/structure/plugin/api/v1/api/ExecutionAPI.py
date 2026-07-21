from typing import Protocol

from structure.plugin.api.v1.model import ExecutionRequest


class ExecutionAPI(Protocol):
    def execute(self, request: ExecutionRequest) -> object: ...
