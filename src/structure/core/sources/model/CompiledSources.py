from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from structure.core.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.core.sources.model.SourceTransformAddress import SourceTransformAddress

if TYPE_CHECKING:
    from structure.core.sources.model.StructureSources import StructureSources


@dataclass(frozen=True)
class CompiledSources(Mapping[SourceTransformAddress, CompiledTransform]):
    sources: "StructureSources"
    artifacts: Mapping[SourceTransformAddress, CompiledTransform]

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifacts", MappingProxyType(dict(self.artifacts)))

    def __getitem__(self, address: SourceTransformAddress) -> CompiledTransform:
        return self.artifacts[address]

    def __iter__(self) -> Iterator[SourceTransformAddress]:
        return iter(self.artifacts)

    def __len__(self) -> int:
        return len(self.artifacts)

    def artifact(self, address: SourceTransformAddress | str) -> CompiledTransform:
        parsed = SourceTransformAddress.parse(address)
        try:
            return self.artifacts[parsed]
        except KeyError:
            available = ", ".join(map(str, self.artifacts))
            raise KeyError(f"Unknown compiled transform {parsed}. Available: {available}") from None
