from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.logic.model.GeneratedFileChange import GeneratedFileChange


@dataclass(frozen=True)
class GeneratedFileSetResult:
    changes: tuple[GeneratedFileChange, ...]

    def count(self, status: str) -> int:
        return sum(1 for change in self.changes if change.status == status)

    def changed(self) -> bool:
        return any(change.status != "unchanged" for change in self.changes)
