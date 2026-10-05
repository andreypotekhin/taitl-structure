"""Read-only caller handoff for a declared foreach sink."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SinkResult:
    """The final output DataFrame and sink contract for a named sink."""

    dataframe: object
    writer: type | None = None
    schema: type | None = None
    output: str | None = None
    input_schema: type | None = None

    @property
    def kind(self) -> str:
        return "batch" if self.schema is not None else "row"
