from structure.core.compiler.api.Artifacts import Artifacts
from structure.core.compiler.api.Compileability import Compileability
from structure.core.compiler.diagnostics.api import Diagnostics
from structure.core.compiler.frontend.api import Frontend
from structure.core.compiler.symbolic_execution.api import SymbolicExecution
from structure.core.compiler.traceability.api import Traceability


class Compiler:
    artifacts = Artifacts()
    compileability = Compileability()
    diagnostics = Diagnostics()
    frontend = Frontend()
    symbolic_execution = SymbolicExecution()
    traceability = Traceability()
