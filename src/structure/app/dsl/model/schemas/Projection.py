from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure


@dataclass(frozen=True)
class Projection:
    source: object
    target: type[Structure] | None = None
    fields: tuple[str, ...] | None = None

    def __call__(self, **overrides: object) -> Structure:
        if self.target is None:
            raise TypeError("project(source, fields) cannot accept field overrides")
        values = self.target._base_values((self.source,))
        values.update(overrides)
        return self.target(**values)
