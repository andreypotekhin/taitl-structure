from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OpaqueBoundary:
    step: str
    hook: str
    phase: str
    target: str
    schema: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "hook": self.hook,
            "phase": self.phase,
            "reason": self.reason,
            "schema": self.schema,
            "step": self.step,
            "target": self.target,
        }
