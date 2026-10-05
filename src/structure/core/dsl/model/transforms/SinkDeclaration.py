"""Named declaration of a caller-owned transform sink."""

from __future__ import annotations

from dataclasses import dataclass
from inspect import isabstract

from structure.core.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class SinkDeclaration:
    """A class-level name for a row writer or a batch output schema."""

    sink_type: type
    name: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.sink_type, type):
            raise TypeError("sink(...) requires a Sink subclass or Schema class")
        if issubclass(self.sink_type, Schema):
            return
        if not any(base.__dict__.get("_structure_sink_role", False) for base in self.sink_type.__mro__):
            raise TypeError("sink(writer_type) requires a subclass of structure.plugin.pyspark.Sink")
        if isabstract(self.sink_type):
            raise TypeError("sink(writer_type) requires a concrete Sink subclass that implements process(row)")
        if not callable(getattr(self.sink_type, "process", None)):
            raise TypeError("sink(writer_type) requires an implemented process(row: Row) -> None method")

    @property
    def kind(self) -> str:
        return "batch" if issubclass(self.sink_type, Schema) else "row"

    @property
    def writer_type(self) -> type | None:
        """Return the row writer class for legacy row-sink consumers."""
        return None if self.kind == "batch" else self.sink_type

    @property
    def schema_type(self) -> type[Schema] | None:
        """Return the batch sink's expected output schema, when declared."""
        return self.sink_type if self.kind == "batch" else None

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None) -> SinkDeclaration:
        return self
