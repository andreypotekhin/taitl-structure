from __future__ import annotations

from dataclasses import dataclass

from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model import StreamingSupport


@dataclass(frozen=True)
class StreamingFinding:
    code: str
    support: StreamingSupport
    step: str
    operation: str
    problem: str
    use: str
    docs: str = "docs/reference/StreamingCompatibility.md"

    def to_diagnostic(self) -> Diagnostic:
        return Diagnostic(
            entry=diagnostic_registry[self.code],
            problem=self.problem,
            use=self.use,
            context={
                "step": self.step,
                "operation": self.operation,
                "classification": self.support.value,
            },
        )
