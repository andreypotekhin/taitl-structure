from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from structure.app.target.pyspark.commands.WriteGeneratedFiles import WriteGeneratedFiles
from structure.app.target.pyspark.model.GeneratedFileSetResult import GeneratedFileSetResult


class DiskStorage:

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def write(self, files: Mapping[str, str]) -> GeneratedFileSetResult:
        return WriteGeneratedFiles()(files, root=self.root)
