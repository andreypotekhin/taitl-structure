from __future__ import annotations

import inspect
import sys

from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.sources.model.SourceTransformAddress import SourceTransformAddress
from structure.app.sources.model.StructureSources import _ORIGINS, SourceOrigin, StructureSources


class DiscoverStructureSources:
    def __call__(self, sources: StructureSources) -> dict[SourceTransformAddress, type[Transform]]:
        sources.load()
        discovered: dict[SourceTransformAddress, type[Transform]] = {}
        for module_name, path in sorted(sources.module_paths().items()):
            module = sys.modules[module_name]
            for value in module.__dict__.values():
                if self._source_class(value, module_name):
                    _ORIGINS[value] = SourceOrigin(
                        path=path,
                        digest=self._digest(sources.texts[path]),
                        source_digest=sources.digest,
                    )
                if self._transform(value, module_name):
                    address = SourceTransformAddress(module_name, value.__qualname__)
                    discovered[address] = value
        return discovered

    @staticmethod
    def _transform(value: object, module_name: str) -> bool:
        return (
            isinstance(value, type)
            and issubclass(value, Transform)
            and value is not Transform
            and value.__module__ == module_name
            and not inspect.isabstract(value)
            and (bool(value._structure_outputs) or value._structure_pipeline is not None)
        )

    @staticmethod
    def _source_class(value: object, module_name: str) -> bool:
        return (
            isinstance(value, type)
            and value.__module__ == module_name
            and (issubclass(value, Transform) or issubclass(value, Schema))
        )

    @staticmethod
    def _digest(content: str) -> str:
        import hashlib

        return hashlib.sha256(content.encode()).hexdigest()
