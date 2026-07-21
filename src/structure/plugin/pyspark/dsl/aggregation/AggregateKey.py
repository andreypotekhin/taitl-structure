from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AggregateKey:
    name: str
    expression: Any
