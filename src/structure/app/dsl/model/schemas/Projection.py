from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class Projection:
    source: object
    target: type[Schema] | None = None
    fields: tuple[str, ...] | None = None

    def __call__(self, **overrides: object) -> Schema:
        if self.target is None:
            raise TypeError("project(source, fields) cannot accept field overrides")
        values = self.target._base_values((self.source,))
        values.update(overrides)
        return self.target(**values)
