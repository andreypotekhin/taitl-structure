from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.compileability.streaming_compatibility.logic.model.StreamingSupport import StreamingSupport
from structure.lib.cross.errors import Diagnostic, diagnostic_registry


@dataclass(frozen=True)
class StreamingFinding:
    code: str
    support: StreamingSupport
    step: str
    operation: str
    problem: str
    use: str
    docs: str = "docs/specifications/StreamingCompatibility.md"

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
