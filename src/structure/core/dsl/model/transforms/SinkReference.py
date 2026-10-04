"""Compile-time value passed to a transform step for one declared sink."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SinkReference:
    """Identify a sink without constructing or calling its writer."""

    name: str
    writer_type: type

    def __getattr__(self, name: str) -> object:
        raise TypeError(
            f"Sink parameter {self.name!r} is a compile-time reference; do not call writer methods in a step. "
            "Use foreach(row, sink) to attach the final output."
        )
