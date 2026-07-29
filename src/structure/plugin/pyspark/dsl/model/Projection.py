"""Internal projection record produced by ``pyspark.dsl.body.project``."""

from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema


@dataclass(frozen=True)
class Projection:
    """A symbolic projection returned from a compiled step body.

    The record is deliberately internal: users write ``project(...)`` while the
    symbolic compiler consumes this value to build explicit Spark ``select``
    assignments in schema order.
    """

    sources: tuple[object, ...]
    target: type[Schema] | None = None
    fields: tuple[str, ...] | None = None

    def __call__(self, **overrides: object) -> Schema:
        """Materialize a target schema instance when projection overrides are used."""
        if self.target is None:
            raise TypeError("project(source, fields) cannot accept field overrides")
        values = self.target._project_values(self.sources)
        values.update(overrides)
        return self.target(**values)
