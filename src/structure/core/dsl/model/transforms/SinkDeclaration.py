"""Named declaration of a caller-owned transform sink."""

from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class SinkDeclaration:
    """A named schema consumed by a caller-owned row or batch sink."""

    sink_type: type
    name: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.sink_type, type) or not issubclass(self.sink_type, Schema):
            raise TypeError("sink(schema) requires a Structure Schema class")

    @property
    def schema(self) -> type[Schema]:
        return self.sink_type

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None) -> SinkDeclaration:
        return self
