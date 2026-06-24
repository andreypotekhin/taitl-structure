from structure.app.compiler.traceability.api import BuildCompilerTraceability


class TraceabilityEndpoint:

    def build(self) -> BuildCompilerTraceability:
        return BuildCompilerTraceability()
