from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class SchemaInspectionRequest:
    schema: object | None
    from_path: str | None
    from_table: str | None
    format: str | None
    runtime: object | None
    plugin_options: Mapping[str, object]
    options: Mapping[str, str] | None
