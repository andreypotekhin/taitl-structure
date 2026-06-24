from structure.app.compiler.frontend.logic.actions.CompileTransform import CompileTransform


class FrontendEndpoint:

    def compile(self) -> CompileTransform:
        return CompileTransform()
