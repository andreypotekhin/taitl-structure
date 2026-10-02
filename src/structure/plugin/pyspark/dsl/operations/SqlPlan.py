from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.dsl import Schema


@dataclass(frozen=True)
class SqlPlan:
    statement: str
    relations: tuple[tuple[str, str], ...]
    args: object | None
    schema: type[Schema]
    label: str | None
    scope: str
