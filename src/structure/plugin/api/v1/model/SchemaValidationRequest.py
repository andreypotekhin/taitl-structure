from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class SchemaValidationRequest:
    schemas: tuple[object, ...]
    configuration: Mapping[str, object]
    plugin_options: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
