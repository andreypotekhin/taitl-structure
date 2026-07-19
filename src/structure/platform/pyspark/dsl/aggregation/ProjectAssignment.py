from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProjectAssignment:
    field: Any
    expression: Any
