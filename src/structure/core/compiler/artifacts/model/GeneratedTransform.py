from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class GeneratedTransform:
    source_unit: str
    module_name: str
    classes: tuple[str, ...]
    generated_package: str
    files: Mapping[str, str]
    storage: object
    result: object = None
