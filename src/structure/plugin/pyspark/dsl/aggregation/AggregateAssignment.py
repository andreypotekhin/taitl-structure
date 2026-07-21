from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AggregateAssignment:
    field: Any
    function: str
    expression: Any | None = None
    key: str | None = None
    arguments: tuple[Any, ...] = ()
    filter: Any | None = None
    order_by: Any | None = None
    options: tuple[tuple[str, object], ...] = ()
