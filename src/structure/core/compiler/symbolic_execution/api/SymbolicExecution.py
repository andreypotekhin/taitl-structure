from structure.core.compiler.symbolic_execution.commands.OpenCompileContext import OpenCompileContext
from structure.core.compiler.symbolic_execution.commands.ReadCompileContext import ReadCompileContext


class SymbolicExecution:
    def open(self) -> OpenCompileContext:
        return OpenCompileContext()

    def current(self) -> ReadCompileContext:
        return ReadCompileContext()
