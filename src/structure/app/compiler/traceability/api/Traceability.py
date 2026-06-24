from structure.app.compiler.traceability.commands.BuildCompilerTraceability import BuildCompilerTraceability


class Traceability:

    @staticmethod
    def build() -> BuildCompilerTraceability:
        return BuildCompilerTraceability()
