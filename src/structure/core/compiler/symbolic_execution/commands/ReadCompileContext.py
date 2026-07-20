from structure.platform.api.v1.model import SymbolicContext, current_symbolic_context


class ReadCompileContext:
    def __call__(self) -> SymbolicContext | None:
        return current_symbolic_context()
