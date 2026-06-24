from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.types.StructureType import StructureType


@dataclass(frozen=True)
class DecimalType(StructureType):
    precision: int
    scale: int

    def __init__(self, precision: int, scale: int) -> None:
        if precision < 1:
            raise ValueError("Decimal precision must be positive")
        if scale < 0 or scale > precision:
            raise ValueError("Decimal scale must be between 0 and precision")

        object.__setattr__(self, "name", "decimal")
        object.__setattr__(self, "precision", precision)
        object.__setattr__(self, "scale", scale)
