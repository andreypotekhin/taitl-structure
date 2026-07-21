from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WatermarkPlan:
    expression: Any
    delay: str

    @property
    def scope(self) -> str:
        return str((self.expression.data or {}).get("scope", ""))

    @property
    def column(self) -> str:
        return str((self.expression.data or {}).get("field", ""))
