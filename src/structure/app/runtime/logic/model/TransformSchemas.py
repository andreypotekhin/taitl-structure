from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class TransformSchemas:
    inputs: Mapping[str, object]
    steps: Mapping[str, object]
    output: object
