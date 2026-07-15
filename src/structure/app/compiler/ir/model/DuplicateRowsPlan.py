from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.expr.Expression import Expression


@dataclass(frozen=True)
class DuplicateRowsPlan:
    subset: tuple[Expression, ...] = ()
    scope: str | None = None
    within_watermark: bool = False
