from __future__ import annotations

import importlib
import sys
from pathlib import Path

from structure.app.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.app.configuration.model.StructureConfig import StructureConfig
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.Transform import Transform


class DiscoverStructureProject:

    def __call__(self, config: StructureConfig) -> DiscoveredStructureProject:
        transforms: list[type[Transform]] = []
        schemas: dict[str, list[type[Structure]]] = {}
        for root in config.source_roots:
            self._add_import_root(root)
            for module_name in self._modules(root):
                module = importlib.import_module(module_name)
                for value in module.__dict__.values():
                    if self._transform(value, module_name):
                        transforms.append(value)
                    if self._schema(value, module_name):
                        schemas.setdefault(module_name, []).append(value)
        return DiscoveredStructureProject(
            transforms=tuple(dict.fromkeys(transforms)),
            schema_modules={module: tuple(items) for module, items in sorted(schemas.items())},
        )

    def _add_import_root(self, root: Path) -> None:
        text = str(root)
        if text not in sys.path:
            sys.path.insert(0, text)

    def _modules(self, root: Path) -> tuple[str, ...]:
        modules: list[str] = []
        for path in sorted(root.rglob("*.py")):
            if path.name == "__init__.py":
                continue
            modules.append(".".join(path.relative_to(root).with_suffix("").parts))
        return tuple(modules)

    def _transform(self, value: object, module_name: str) -> bool:
        return (
            isinstance(value, type)
            and issubclass(value, Transform)
            and value is not Transform
            and value.__module__ == module_name
            and bool(getattr(value, "_structure_transform", False))
        )

    def _schema(self, value: object, module_name: str) -> bool:
        return (
            isinstance(value, type)
            and issubclass(value, Structure)
            and value is not Structure
            and value.__module__ == module_name
        )


discover_structure_project = DiscoverStructureProject()
