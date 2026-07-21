from __future__ import annotations

from builtins import type as class_type
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Mapping

@dataclass(frozen=True)
class FieldDefinition:
    name: str
    type: object | None
    hint: object | None = None
    nullable: bool = True
    alias: str | None = None
    metadata: Mapping[str, object] = dataclass_field(default_factory=dict)
    description: str | None = None
    validator: Callable[[class_type, Mapping[str, "FieldDefinition"]], None] | None = dataclass_field(
        default=None, repr=False, compare=False
    )

    @property
    def column(self) -> str:
        return self.alias or self.name
