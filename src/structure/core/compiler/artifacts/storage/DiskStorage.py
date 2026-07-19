from __future__ import annotations

import importlib
import sys
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType

from structure.core.compiler.artifacts.commands.WriteGeneratedFiles import WriteGeneratedFiles
from structure.core.compiler.artifacts.model.GeneratedFileSetResult import GeneratedFileSetResult


class DiskStorage:

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def write(self, files: Mapping[str, str]) -> GeneratedFileSetResult:
        return WriteGeneratedFiles()(files, root=self.root)

    def import_module(self, module_name: str) -> ModuleType:
        root = str(self.root)
        if root not in sys.path:
            sys.path.insert(0, root)
        return importlib.import_module(module_name)
