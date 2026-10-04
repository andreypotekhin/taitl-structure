"""Read-only caller handoff for a declared foreach sink."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SinkResult:
    """The final output DataFrame and opaque writer class for a named sink."""

    dataframe: object
    writer: type
