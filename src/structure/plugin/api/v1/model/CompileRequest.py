from dataclasses import dataclass
from typing import Mapping

from structure.plugin.api.v1.model.CompilationPurpose import CompilationPurpose


@dataclass(frozen=True)
class CompileRequest:
    transform: object
    target: str
    configuration: Mapping[str, object]
    analysis: object | None = None
    purpose: CompilationPurpose = CompilationPurpose.RUNTIME
