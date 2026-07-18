from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class EngineManifest:
    requires_structure: str
    core_engine_revision: str
    replacements: Mapping[type, type]
