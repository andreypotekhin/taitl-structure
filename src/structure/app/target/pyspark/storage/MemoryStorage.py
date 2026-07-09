from __future__ import annotations

import sys
from collections.abc import Mapping
from types import ModuleType


class MemoryStorage:

    def __init__(self) -> None:
        self.files: dict[str, str] = {}
        self.modules: dict[str, ModuleType] = {}

    def write(self, files: Mapping[str, str]) -> None:
        self.files.update(dict(files))
        self._install_packages(files)
        for path in self._module_paths(files):
            module = self._module(path)
            module.__dict__.clear()
            module.__dict__.update(
                {
                    "__builtins__": __builtins__,
                    "__file__": f"<structure-memory:{path}>",
                    "__name__": self._module_name(path),
                    "__package__": self._module_name(path).rsplit(".", 1)[0],
                }
            )
            exec(compile(files[path], module.__dict__["__file__"], "exec"), module.__dict__)

    def import_module(self, module_name: str) -> ModuleType:
        try:
            return self.modules[module_name]
        except KeyError:
            raise ImportError(f"MemoryStorage does not contain generated module {module_name}")

    def _install_packages(self, files: Mapping[str, str]) -> None:
        packages = [path for path in files if path.endswith("/__init__.py")]
        for path in sorted(packages, key=lambda item: item.count("/")):
            module_name = self._package_name(path)
            module = self.modules.get(module_name) or sys.modules.get(module_name) or ModuleType(module_name)
            module.__path__ = []  # type: ignore[attr-defined]
            self.modules[module_name] = module
            sys.modules[module_name] = module
            self._attach(module_name, module)

    def _module_paths(self, files: Mapping[str, str]) -> tuple[str, ...]:
        paths = [path for path in files if path.endswith(".py") and not path.endswith("/__init__.py")]
        return tuple(sorted(paths, key=self._module_order))

    def _module_order(self, path: str) -> tuple[int, str]:
        if "/runtime/" in path:
            return (0, path)
        if "/schemas/" in path:
            return (1, path)
        if "/transforms/" in path:
            return (2, path)
        return (3, path)

    def _module(self, path: str) -> ModuleType:
        module_name = self._module_name(path)
        module = self.modules.get(module_name) or ModuleType(module_name)
        self.modules[module_name] = module
        sys.modules[module_name] = module
        self._attach(module_name, module)
        return module

    def _attach(self, module_name: str, module: ModuleType) -> None:
        if "." not in module_name:
            return
        parent_name, child_name = module_name.rsplit(".", 1)
        parent = sys.modules.get(parent_name)
        if parent is not None:
            setattr(parent, child_name, module)

    def _package_name(self, path: str) -> str:
        return path.removesuffix("/__init__.py").replace("/", ".")

    def _module_name(self, path: str) -> str:
        return path.removesuffix(".py").replace("/", ".")
