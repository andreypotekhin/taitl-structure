"""Read-only caller handoff for a declared foreach sink."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SinkResult:
    """The final output DataFrame and consumed schema for a named sink."""

    dataframe: object
    schema: type
    output: str
    kind: str
    name: str
