from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class DiscoveredStructureProject:
    transforms: tuple[type[Transform], ...]
    schema_modules: dict[str, tuple[type[Schema], ...]]

    def schemas(self) -> Sequence[type[Schema]]:
        return tuple(schema for schemas in self.schema_modules.values() for schema in schemas)
