from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OutputPlan:
    """A Core-resolved final output; target payload fields remain opaque."""

    name: str
    schema: Any
    source: str
    source_scope: str
    source_schema: Any
    ordinal: int
    aliases: tuple[str, ...] = ()
    streaming: bool = False
    binding: str = "dataframe"
    table_source: str | None = None
