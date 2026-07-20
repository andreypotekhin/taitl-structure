from structure.core.compiler.symbolic_execution.model.CompileContext import CompileContext


class OpenCompileContext:
    def __call__(self, *, step: str, capture_special_exprs: bool = False) -> CompileContext:
        return CompileContext(step=step, capture_special_exprs=capture_special_exprs)
