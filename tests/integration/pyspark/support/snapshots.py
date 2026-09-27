from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any

from structure.core.runtime.session.model.TransformResult import TransformResult


class Snapshots:
    """Eager test-only lineage boundaries on storage shared with Spark workers.

    Parquet preserves values and data types, but may widen schema nullability.
    Returned frames are new relations; the original frames are never modified.
    """

    def __init__(self, spark: Any, root: Path) -> None:
        self.spark = spark
        self.root = root
        self.count = 0

    def __call__(self, frame: Any, *, label: str) -> Any:
        self.count += 1
        path = str(self.root / str(self.count))
        started = perf_counter()
        print(f"[snapshot] {label}: starting", flush=True)
        frame.write.parquet(path)
        result = self.spark.read.parquet(path)
        print(f"[snapshot] {label}: {perf_counter() - started:.2f}s", flush=True)
        return result

    def outputs(self, result: TransformResult, *, label: str) -> TransformResult:
        """Return snapshots by canonical public name, without intermediate stages."""
        outputs = {name: self(frame, label=f"{label}.{name}") for name, frame in result.items()}
        return TransformResult(outputs, schema=result.schema, stage_outputs_enabled=False)
