from __future__ import annotations

from dataclasses import dataclass

from structure.app.streaming.logic.model.StreamingFinding import StreamingFinding
from structure.app.streaming.logic.model.StreamingSupport import StreamingSupport


@dataclass(frozen=True)
class StreamingReport:
    transform: str
    support: StreamingSupport
    required: bool
    findings: tuple[StreamingFinding, ...] = ()

    def compatible(self) -> bool:
        return self.support is StreamingSupport.COMPATIBLE
