from __future__ import annotations

import importlib
from types import ModuleType


class PackageImportStorage:

    def __init__(self, package: str) -> None:
        self.package = package

    def import_module(self, module_name: str) -> ModuleType:
        if module_name != self.package and not module_name.startswith(f"{self.package}."):
            raise ImportError(f"Generated module {module_name} is outside package {self.package}")
        return importlib.import_module(module_name)
