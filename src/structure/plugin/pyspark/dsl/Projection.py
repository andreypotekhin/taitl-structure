from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema


@dataclass(frozen=True)
class Projection:
    sources: tuple[object, ...]
    target: type[Schema] | None = None
    fields: tuple[str, ...] | None = None

    def __call__(self, **overrides: object) -> Schema:
        if self.target is None:
            raise TypeError("project(source, fields) cannot accept field overrides")
        values = self.target._project_values(self.sources)
        values.update(overrides)
        return self.target(**values)
