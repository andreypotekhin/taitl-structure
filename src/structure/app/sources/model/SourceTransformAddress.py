from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class SourceTransformAddress:
    module: str
    qualname: str

    @classmethod
    def parse(cls, value: "SourceTransformAddress | str") -> "SourceTransformAddress":
        if not isinstance(value, str):
            return value
        module, separator, qualname = value.partition(":")
        if not separator or not module or not qualname:
            raise ValueError("Transform address must use python.module:ClassName.")
        return cls(module=module, qualname=qualname)

    def __str__(self) -> str:
        return f"{self.module}:{self.qualname}"
