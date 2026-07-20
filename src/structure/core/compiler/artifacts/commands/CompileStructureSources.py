from __future__ import annotations

from collections.abc import Callable

from structure.core.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.core.sources.api import Sources
from structure.core.sources.model.CompiledSources import CompiledSources
from structure.core.sources.model.StructureSources import StructureSources


class CompileStructureSources:
    def __call__(
        self, sources: StructureSources, *, compile_one: Callable[[type], CompiledTransform]
    ) -> CompiledSources:
        transforms = Sources().discover()(sources)
        return CompiledSources(
            sources=sources, artifacts={address: compile_one(transform) for address, transform in transforms.items()}
        )
