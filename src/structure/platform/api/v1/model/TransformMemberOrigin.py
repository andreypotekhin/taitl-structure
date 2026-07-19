from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TransformMemberOrigin:
    """Stable source identity for a Core-owned transform member."""

    module: str
    class_name: str
    member_name: str
    owner: object | None = None

    @classmethod
    def of(cls, owner: type, member_name: str) -> "TransformMemberOrigin":
        return cls(module=owner.__module__, class_name=owner.__name__, member_name=member_name, owner=owner)

    @property
    def import_name(self) -> str:
        return f"{self.module}.{self.class_name}"
