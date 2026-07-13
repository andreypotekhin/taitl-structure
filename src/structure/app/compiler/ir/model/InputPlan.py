from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.transforms.StreamingMode import StreamingMode


@dataclass(frozen=True)
class InputPlan:
    name: str
    schema: type[Schema]
    ordinal: int
    streaming: StreamingMode = StreamingMode.NO
    aliases: tuple[str, ...] = ()
