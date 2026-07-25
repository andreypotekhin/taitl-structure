from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class GenerationResult:
    """Target-owned generated files and their importable transform module."""

    files: Mapping[str, str]
    module_name: str
    classes: tuple[str, ...]
