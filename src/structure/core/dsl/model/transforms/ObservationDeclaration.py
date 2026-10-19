"""Named declaration of typed batch observation metrics."""

from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class ObservationDeclaration:
    """A named metric schema exposed on a transform result."""

    metric_schema: type[Schema]
    name: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.metric_schema, type) or not issubclass(self.metric_schema, Schema):
            raise TypeError("observation(schema) requires a Structure Schema class")

    @property
    def schema(self) -> type[Schema]:
        return self.metric_schema

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None) -> ObservationDeclaration:
        return self
