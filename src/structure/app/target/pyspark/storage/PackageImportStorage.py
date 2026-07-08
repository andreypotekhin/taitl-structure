from __future__ import annotations

import importlib
from types import ModuleType


class PackageImportStorage:

    def __init__(self, package: str) -> None:
        self.package = package

    def import_module(self, module_name: str) -> ModuleType:
        return importlib.import_module(module_name)

