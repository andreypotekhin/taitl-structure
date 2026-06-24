from structure.app.compiler.api.CompileabilityEndpoint import CompileabilityEndpoint
from structure.app.compiler.api.FrontendEndpoint import FrontendEndpoint
from structure.app.compiler.api.TraceabilityEndpoint import TraceabilityEndpoint


class CompilerEndpoint:

    def __init__(self) -> None:
        self.compileability = CompileabilityEndpoint()
        self.frontend = FrontendEndpoint()
        self.traceability = TraceabilityEndpoint()
