from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.compileability.streaming_compatibility.model.StreamingFinding import StreamingFinding
from structure.app.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport


@dataclass(frozen=True)
class StreamingReport:
    transform: str
    support: StreamingSupport
    required: bool
    findings: tuple[StreamingFinding, ...] = ()

    def compatible(self) -> bool:
        return self.support is StreamingSupport.COMPATIBLE
