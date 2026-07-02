from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from structure.lib.cross.errors import Diagnostic, diagnostic_registry, render_diagnostic


@dataclass(frozen=True)
class RuntimeDiagnostic:
    code: str
    title: str
    transform: str
    execution_mode: str
    target_backend: str
    problem: str
    use: str
    docs: str
    target_profile: str = ">=3.5,<4.1"
    target_variant: str = "ordinary"
    context: Mapping[str, str] = field(default_factory=dict)

    def to_diagnostic(self) -> Diagnostic:
        context = {
            "transform": self.transform,
            "execution_mode": self.execution_mode,
            "target_backend": self.target_backend,
            "target_profile": self.target_profile,
            "target_variant": self.target_variant,
        }
        context.update(self.context)
        return Diagnostic(
            entry=diagnostic_registry[self.code],
            problem=self.problem,
            use=self.use,
            context=context,
        )

    def render(self) -> str:
        return render_diagnostic(self.to_diagnostic(), kind="RuntimeError")
