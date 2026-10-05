"""Compile-time value passed to a transform step for one declared sink."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SinkReference:
    """Identify a declared sink without constructing or calling it."""

    name: str
    sink_type: type
    kind: str = "row"

    @property
    def writer_type(self) -> type:
        """Keep the row writer spelling available to existing plugin helpers."""
        return self.sink_type

    def __getattr__(self, name: str) -> object:
        raise TypeError(
            f"Sink parameter {self.name!r} is a compile-time reference; do not call writer methods in a step. "
            "Use foreach(row, sink) or foreach_batch(row, sink) to attach the final output."
        )
