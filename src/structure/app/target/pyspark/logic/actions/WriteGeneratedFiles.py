from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from structure.app.target.pyspark.logic.model.GeneratedFileChange import GeneratedFileChange
from structure.app.target.pyspark.logic.model.GeneratedFileSetResult import GeneratedFileSetResult


class WriteGeneratedFiles:

    def __call__(self, files: Mapping[str, str], *, root: Path | str) -> GeneratedFileSetResult:
        root_path = Path(root)
        changes = tuple(self._write(path, text, root=root_path) for path, text in sorted(files.items()))
        return GeneratedFileSetResult(changes)

    def _write(self, path: str, text: str, *, root: Path) -> GeneratedFileChange:
        target = root / Path(path)
        if target.exists() and target.read_text(encoding="utf-8") == text:
            return GeneratedFileChange(path, "unchanged")

        status = "modified" if target.exists() else "added"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        return GeneratedFileChange(path, status)


write_generated_files = WriteGeneratedFiles()
