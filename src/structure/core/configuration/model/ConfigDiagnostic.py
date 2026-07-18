from __future__ import annotations

from dataclasses import dataclass

from structure.lib.cross.errors import Diagnostic, diagnostic_registry, render_diagnostic


@dataclass(frozen=True)
class ConfigDiagnostic:
    code: str
    setting: str
    problem: str
    use: str

    @property
    def title(self) -> str:
        return diagnostic_registry[self.code].title

    @property
    def docs(self) -> str:
        return diagnostic_registry[self.code].docs

    def to_diagnostic(self) -> Diagnostic:
        return Diagnostic(
            entry=diagnostic_registry[self.code],
            problem=self.problem,
            use=self.use,
            context={"setting": self.setting},
        )

    def render(self) -> str:
        return render_diagnostic(self.to_diagnostic(), kind="ConfigError")
