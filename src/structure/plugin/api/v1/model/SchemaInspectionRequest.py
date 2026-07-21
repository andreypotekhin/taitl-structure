from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class SchemaInspectionRequest:
    schema: object | None
    from_path: str | None
    from_table: str | None
    format: str | None
    runtime: object | None
    target_variant: str | None
    options: Mapping[str, str] | None
