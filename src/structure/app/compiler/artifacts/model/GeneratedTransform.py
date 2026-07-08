from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class GeneratedTransform:
    generated_package: str
    files: Mapping[str, str]
    storage: object

