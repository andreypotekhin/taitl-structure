from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from structure.app.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class CompilerTransformMember:
    owner: type[Transform]
    name: str
    member: Callable
    position: int
    overridden: tuple["CompilerTransformMember", ...] = ()

    @property
    def key(self) -> tuple[type[Transform], str, int]:
        return self.owner, self.name, self.position

    @property
    def source(self) -> str:
        return f"{self.owner.__module__}.{self.owner.__name__}.{self.name}"
