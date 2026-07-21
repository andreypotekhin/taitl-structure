from __future__ import annotations

import importlib
import inspect
import sys
from pathlib import Path

from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.Transform import Transform


class DiscoverStructureProject:

    def __call__(self, config: StructureConfig) -> DiscoveredStructureProject:
        transforms: list[type[Transform]] = []
        schemas: dict[str, list[type[Schema]]] = {}
        for root in config.source_roots:
            import_root, package = self._import_root(root)
            self._add_import_root(import_root)
            for module_name in self._modules(root, package):
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

    def _import_root(self, root: Path) -> tuple[Path, tuple[str, ...]]:
        package: list[str] = []
        import_root = root
        while (import_root / "__init__.py").is_file():
            package.append(import_root.name)
            import_root = import_root.parent
        return import_root, tuple(reversed(package))

    def _modules(self, root: Path, package: tuple[str, ...]) -> tuple[str, ...]:
        modules: list[str] = []
        for path in sorted(root.rglob("*.py")):
            if path.name == "__init__.py":
                continue
            modules.append(".".join((*package, *path.relative_to(root).with_suffix("").parts)))
        return tuple(modules)

    def _transform(self, value: object, module_name: str) -> bool:
        return (
            isinstance(value, type)
            and issubclass(value, Transform)
            and value is not Transform
            and value.__module__ == module_name
            and not inspect.isabstract(value)
            and self._entrypoint(value)
        )

    def _entrypoint(self, value: type[Transform]) -> bool:
        return bool(value._structure_outputs) or value._structure_pipeline is not None

    def _schema(self, value: object, module_name: str) -> bool:
        return (
            isinstance(value, type)
            and issubclass(value, Schema)
            and value is not Schema
            and value.__module__ == module_name
        )


discover_structure_project = DiscoverStructureProject()
