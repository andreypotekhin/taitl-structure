from __future__ import annotations

from structure.app.runtime.session.logic.model.RuntimeDiagnostic import RuntimeDiagnostic


class StructureRuntimeError(RuntimeError):

    def __init__(self, diagnostic: RuntimeDiagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.render())
