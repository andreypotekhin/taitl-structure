from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from structure.plugin.api.v1.model.CompilationPurpose import CompilationPurpose


@dataclass(frozen=True)
class CompileRequest:
    transform: object
    target: str
    configuration: Mapping[str, object]
    plugin_options: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    analysis: object | None = None
    purpose: CompilationPurpose = CompilationPurpose.RUNTIME
