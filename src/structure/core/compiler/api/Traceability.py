from structure.core.compiler.traceability.api import BuildCompilerTraceability
from structure.core.compiler.traceability.api import Traceability as TraceabilityApp


class Traceability:
    compiler = TraceabilityApp()

    def build(self) -> BuildCompilerTraceability:
        return self.compiler.build()
