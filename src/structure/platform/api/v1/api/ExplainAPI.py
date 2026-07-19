from typing import Protocol

from structure.platform.api.v1.model import ExplainRequest


class ExplainAPI(Protocol):
    def render(self, request: ExplainRequest) -> str: ...
