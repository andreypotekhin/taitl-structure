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
    filters: tuple[Any, ...]
    projection: tuple[Any, ...]
    ordinal: int
    joins: tuple[Any, ...] = ()
    operations: tuple[Any, ...] = ()
    aliases: tuple[str, ...] = ()
