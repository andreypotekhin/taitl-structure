from structure.app.compiler.traceability.api import BuildCompilerTraceability


class Traceability:

    @staticmethod
    def build() -> BuildCompilerTraceability:
        return BuildCompilerTraceability()
