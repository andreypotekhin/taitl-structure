from __future__ import annotations

from structure.lib.cross.errors import Diagnostic, render_diagnostic


class StructureCompileError(TypeError):

    def __init__(self, diagnostic: Diagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(render_diagnostic(diagnostic, kind="CompileError"))
