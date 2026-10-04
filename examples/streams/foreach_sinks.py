"""Small caller-configured writers for row-level foreach examples."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from structure import special

if TYPE_CHECKING:
    from pyspark.sql import Row


@special(type="opaque")
class JsonLinesAlertWriter:
    """Append streaming rows and lifecycle markers to one file per task epoch."""

    def __init__(self, destination: str | Path) -> None:
        self.destination = Path(destination)
        self._path: Path | None = None

    def open(self, partition_id: int, epoch_id: int) -> bool:
        self.destination.mkdir(parents=True, exist_ok=True)
        self._path = self.destination / f"partition-{partition_id}-epoch-{epoch_id}.jsonl"
        self._append({"event": "open", "partition_id": partition_id, "epoch_id": epoch_id})
        return True

    def process(self, row: Row) -> None:
        self._append({"event": "process", "row": row.asDict(recursive=True)})

    def close(self, error: Exception | None) -> None:
        self._append({"event": "close", "error": str(error) if error is not None else None})

    def _append(self, value: dict[str, object]) -> None:
        if self._path is None:
            raise RuntimeError("open(...) must set the foreach partition file before process(...) or close(...)")
        with self._path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, sort_keys=True) + "\n")
