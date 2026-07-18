from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class CompileRequest:
    transform: object
    target: str
    configuration: Mapping[str, object]
