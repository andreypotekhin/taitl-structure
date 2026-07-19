from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class SchemaValidationRequest:
    schemas: tuple[object, ...]
    configuration: Mapping[str, object]
