from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.Transform import Transform


@dataclass(frozen=True)
class DiscoveredStructureProject:
    transforms: tuple[type[Transform], ...]
    schema_modules: dict[str, tuple[type[Structure], ...]]

    def schemas(self) -> Sequence[type[Structure]]:
        return tuple(schema for schemas in self.schema_modules.values() for schema in schemas)
