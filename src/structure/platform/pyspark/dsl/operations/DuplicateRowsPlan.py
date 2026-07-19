from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DuplicateRowsPlan:
    subset: tuple[Any, ...] = ()
    scope: str | None = None
    within_watermark: bool = False
