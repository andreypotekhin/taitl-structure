from __future__ import annotations

from structure.core.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic


class StructureRuntimeError(RuntimeError):

    def __init__(self, diagnostic: RuntimeDiagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.render())
