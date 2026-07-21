from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class GenerationRequest:
    payload: object
    source_module: str | None = None
    source_schema_modules: object | None = None
    generated_package: str | None = None
    semantic_fingerprints: Mapping[str, str] | None = None
    generated_code_options: tuple[str, ...] = ()
