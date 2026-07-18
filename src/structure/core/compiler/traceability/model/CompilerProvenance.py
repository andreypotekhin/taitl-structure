from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompilerProvenance:
    source: str
    ir: str
    generated: str

    def to_dict(self) -> dict[str, str]:
        return {
            "generated": self.generated,
            "ir": self.ir,
            "source": self.source,
        }
