from __future__ import annotations

import hashlib
import importlib
import importlib.abc
import importlib.util
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from threading import RLock
from types import MappingProxyType, ModuleType

_LOCK = RLock()


@dataclass(frozen=True)
class SourceOrigin:
    path: str
    digest: str
    source_digest: str


_ORIGINS: dict[type, SourceOrigin] = {}


def source_origin(value: type) -> SourceOrigin | None:
    return _ORIGINS.get(value)


@dataclass(frozen=True)
class StructureSources:
    texts: Mapping[str, str]
    digest: str = field(init=False)
    _modules: Mapping[str, str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        files = {self._path(path): text for path, text in self.texts.items()}
        if len(files) != len(self.texts):
            raise ValueError("Structure source paths must be unique after normalization.")
        if not all(isinstance(text, str) for text in files.values()):
            raise TypeError("Structure source content must be text.")
        ordered = dict(sorted(files.items()))
        modules = {self._module(path): path for path in ordered}
        if len(modules) != len(ordered):
            raise ValueError("Structure source paths must map to unique Python modules.")
        digest = hashlib.sha256("".join(f"{path}\0{text}\0" for path, text in ordered.items()).encode()).hexdigest()
        object.__setattr__(self, "texts", MappingProxyType(ordered))
        object.__setattr__(self, "_modules", MappingProxyType(modules))
        object.__setattr__(self, "digest", digest)

    @classmethod
    def files(cls, files: Mapping[str, str]) -> "StructureSources":
        return cls(texts=files)

    @classmethod
    def from_directory(cls, directory: Path | str) -> "StructureSources":
        root = Path(directory)
        if not root.is_dir():
            raise ValueError(f"Structure source directory does not exist: {root}")
        return cls.files(
            {path.relative_to(root).as_posix(): path.read_text(encoding="utf-8") for path in root.rglob("*.py")}
        )

    def load(self) -> None:
        with _LOCK:
            finder = _SourceFinder(self)
            if not any(isinstance(item, _SourceFinder) and item.sources is self for item in sys.meta_path):
                sys.meta_path.insert(0, finder)
            self._prepare_modules()
            for module in self._modules:
                importlib.import_module(module)

    def path_for(self, module: str) -> str:
        return self._modules[module]

    def module_paths(self) -> Mapping[str, str]:
        return self._modules

    def _prepare_modules(self) -> None:
        names = {part for module in self._modules for part in self._parents(module)}
        for module in sorted(names, key=lambda item: item.count("."), reverse=True):
            existing = sys.modules.get(module)
            if existing is None or getattr(existing, "__structure_sources_digest__", None) == self.digest:
                continue
            if not hasattr(existing, "__structure_sources_digest__"):
                raise ValueError(
                    f"Source module {module} is already loaded by the application. Use a distinct package root."
                )
            sys.modules.pop(module, None)

    @staticmethod
    def _parents(module: str) -> Iterable[str]:
        parts = module.split(".")
        return (".".join(parts[:index]) for index in range(1, len(parts) + 1))

    @staticmethod
    def _path(value: str) -> str:
        path = PurePosixPath(value)
        if not value or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError(f"Invalid Structure source path: {value!r}")
        if path.suffix != ".py":
            raise ValueError(f"Structure source path must end in .py: {value}")
        parts = path.with_suffix("").parts
        if not all(part == "__init__" or part.isidentifier() for part in parts):
            raise ValueError(f"Structure source path must contain Python module names: {value}")
        return path.as_posix()

    @staticmethod
    def _module(path: str) -> str:
        parts = list(PurePosixPath(path).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        if not parts:
            raise ValueError("A root __init__.py cannot be a Structure source module.")
        return ".".join(parts)


class _SourceFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, sources: StructureSources) -> None:
        self.sources = sources

    def find_spec(self, fullname: str, path=None, target=None):
        source_path = self.sources.module_paths().get(fullname)
        if source_path is not None:
            return importlib.util.spec_from_loader(fullname, self, is_package=source_path.endswith("/__init__.py"))
        if any(module.startswith(f"{fullname}.") for module in self.sources.module_paths()):
            return importlib.util.spec_from_loader(fullname, self, is_package=True)
        return None

    def create_module(self, spec):
        return None

    def exec_module(self, module: ModuleType) -> None:
        path = self.sources.module_paths().get(module.__name__)
        module.__dict__["__structure_sources_digest__"] = self.sources.digest
        if path is None:
            module.__path__ = []  # type: ignore[attr-defined]
            return
        code = compile(self.sources.texts[path], f"<structure-sources:{path}>", "exec")
        exec(code, module.__dict__)
