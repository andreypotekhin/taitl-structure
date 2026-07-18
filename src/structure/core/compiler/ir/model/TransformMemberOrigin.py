from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class TransformMemberOrigin:
    module: str
    class_name: str
    member_name: str
    owner: type[Transform] | None = None

    @classmethod
    def of(cls, owner: type[Transform], member_name: str) -> "TransformMemberOrigin":
        return cls(
            module=owner.__module__,
            class_name=owner.__name__,
            member_name=member_name,
            owner=owner,
        )

    @property
    def import_name(self) -> str:
        return f"{self.module}.{self.class_name}"
