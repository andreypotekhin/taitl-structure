from __future__ import annotations

from dataclasses import dataclass

from structure.app.backend.capabilities.logic.model.BackendDiagnostic import BackendDiagnostic
from structure.app.backend.capabilities.logic.model.BackendDiagnosticCodes import BACKEND_E2401, BACKEND_E2402
from structure.app.backend.capabilities.logic.model.BackendId import BackendId
from structure.app.backend.capabilities.logic.model.CapabilityRequirement import CapabilityRequirement


@dataclass(frozen=True)
class CapabilityDecision:
    backend: BackendId
    requirement: CapabilityRequirement
    supported: bool
    code: str = ""
    title: str = ""
    problem: str = ""
    why: str = ""
    use: str = ""
    docs: str = ""
    required_target: str = ""

    @staticmethod
    def ok(*, backend: BackendId, requirement: CapabilityRequirement) -> "CapabilityDecision":
        return CapabilityDecision(
            backend=backend,
            requirement=requirement,
            supported=True,
            docs=requirement.docs,
        )

    @staticmethod
    def unsupported_backend(
        *,
        backend: BackendId,
        requirement: CapabilityRequirement,
        supported_backend: str,
    ) -> "CapabilityDecision":
        return CapabilityDecision(
            backend=backend,
            requirement=requirement,
            supported=False,
            code=BACKEND_E2401,
            title="Unsupported backend target",
            problem=f"Structure has no backend capability profile for target_backend = {backend.name!r}.",
            why="Compiler, online runtime, and generated output need a known static capability profile.",
            use=f"Set target_backend = {supported_backend!r}.",
            docs=requirement.docs,
            required_target=supported_backend,
        )

    @staticmethod
    def unsupported_capability(
        *,
        backend: BackendId,
        requirement: CapabilityRequirement,
        rationale: str,
        use: str,
        required_target: str = "",
    ) -> "CapabilityDecision":
        return CapabilityDecision(
            backend=backend,
            requirement=requirement,
            supported=False,
            code=BACKEND_E2402,
            title="Unsupported backend capability",
            problem=(
                f"{backend.display()} does not support {requirement.group}.{requirement.name} "
                "through Structure's compiler-visible backend contract."
            ),
            why=rationale,
            use=use,
            docs=requirement.docs,
            required_target=required_target,
        )

    def to_diagnostic(self) -> BackendDiagnostic:
        return BackendDiagnostic(
            code=self.code,
            title=self.title,
            backend=self.backend.name,
            target=self.backend.target,
            feature_group=self.requirement.group,
            feature_name=self.requirement.name,
            problem=self.problem,
            why=self.why,
            use=self.use,
            docs=self.docs,
            source=dict(self.requirement.source),
        )
