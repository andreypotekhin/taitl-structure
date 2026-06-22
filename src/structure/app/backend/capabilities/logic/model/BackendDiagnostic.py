from dataclasses import dataclass, field

from structure.lib.cross.errors import Diagnostic, diagnostic_registry


@dataclass(frozen=True)
class BackendDiagnostic:
    code: str
    title: str
    backend: str
    target: str
    feature_group: str
    feature_name: str
    problem: str
    why: str
    use: str
    docs: str
    source: dict[str, str] = field(default_factory=dict)

    def context(self) -> dict[str, str]:
        context = {
            "target_backend": self.backend,
            "target": self.target,
            "feature_group": self.feature_group,
            "feature_name": self.feature_name,
        }
        context.update(self.source)
        return context

    def to_diagnostic(self) -> Diagnostic:
        return Diagnostic(
            entry=diagnostic_registry[self.code],
            problem=self.problem,
            use=self.use,
            context=self.context(),
        )
