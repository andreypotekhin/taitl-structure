from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.dsl.windows.WindowBound import WindowBound


@dataclass(frozen=True)
class WindowFrame:
    kind: str
    start: WindowBound
    end: WindowBound
