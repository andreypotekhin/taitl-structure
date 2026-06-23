from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class TransformSchemas:
    inputs: Mapping[str, object]
    steps: Mapping[str, object]
    outputs: Mapping[str, object]

    @property
    def output(self) -> object:
        if len(self.outputs) != 1:
            names = ", ".join(self.outputs)
            raise AttributeError(f"Transform has multiple outputs: {names}. Use schemas.outputs[...] instead.")
        return next(iter(self.outputs.values()))
