from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from structure.app.dsl.logic.model.types.StructureType import StructureType


@dataclass(frozen=True)
class Expression:
    kind: str
    type: StructureType | None = None
    nullable: bool = True
    data: Mapping[str, object] | None = None
    args: tuple["Expression", ...] = ()

    def is_null(self) -> "Expression":
        return Expression(kind="is_null", type=None, nullable=False, args=(self,))

    def is_not_null(self) -> "Expression":
        return Expression(kind="is_not_null", type=None, nullable=False, args=(self,))

    def __bool__(self) -> bool:
        raise TypeError("Structure expressions cannot be used as Python booleans. Use where(...), &, |, or ~.")
