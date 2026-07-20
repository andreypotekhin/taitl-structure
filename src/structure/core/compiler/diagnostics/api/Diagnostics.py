from structure.core.compiler.diagnostics.commands.BuildCompilerDiagnosticSource import BuildCompilerDiagnosticSource


class Diagnostics:
    def source(self) -> BuildCompilerDiagnosticSource:
        return BuildCompilerDiagnosticSource()
