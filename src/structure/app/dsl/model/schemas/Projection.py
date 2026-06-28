from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure


@dataclass(frozen=True)
class Projection:
    source: object
    target: type[Structure] | None = None
    fields: tuple[str, ...] | None = None
