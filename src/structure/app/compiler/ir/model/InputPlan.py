from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.StreamingMode import StreamingMode


@dataclass(frozen=True)
class InputPlan:
    name: str
    schema: type[Structure]
    ordinal: int
    streaming: StreamingMode = StreamingMode.NO
