"""Named declaration of an opaque row writer used by a transform step."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SinkDeclaration:
    """A class-level name for a row writer supplied to a step method."""

    writer_type: type
    name: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.writer_type, type):
            raise TypeError("sink(...) requires a writer class")
        if getattr(self.writer_type, "_structure_special_type", None) != "opaque":
            raise TypeError('sink(writer_type) requires a class decorated with @special(type="opaque")')
        if not callable(getattr(self.writer_type, "process", None)):
            raise TypeError("sink(writer_type) requires a callable process(row) method")

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None) -> SinkDeclaration:
        return self
