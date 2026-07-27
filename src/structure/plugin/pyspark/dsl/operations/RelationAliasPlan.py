from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema


@dataclass(frozen=True)
class RelationAliasPlan:
    input_name: str
    source: str
    schema: type[Schema]
    alias: str
