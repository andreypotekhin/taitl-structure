from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.windows.WindowFrame import WindowFrame


@dataclass(frozen=True)
class WindowSpec:
    partition_by: tuple[Expression, ...]
    order_by: tuple[Expression, ...]
    frame: WindowFrame | None = None
