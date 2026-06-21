from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class CapabilityRequirement:
    group: str
    name: str
    source: Mapping[str, str] = field(default_factory=dict)
    docs: str = "docs/specifications/BackendCapabilities.md"

    def key(self) -> tuple[str, str]:
        return self.group, self.name
