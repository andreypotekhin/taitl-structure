from __future__ import annotations

from collections.abc import Callable

from structure.app.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.app.sources.commands.DiscoverStructureSources import DiscoverStructureSources
from structure.app.sources.model.CompiledSources import CompiledSources
from structure.app.sources.model.StructureSources import StructureSources


class CompileStructureSources:
    def __init__(self) -> None:
        self._discover = DiscoverStructureSources()

    def __call__(
        self, sources: StructureSources, *, compile_one: Callable[[type], CompiledTransform]
    ) -> CompiledSources:
        transforms = self._discover(sources)
        return CompiledSources(
            sources=sources, artifacts={address: compile_one(transform) for address, transform in transforms.items()}
        )
