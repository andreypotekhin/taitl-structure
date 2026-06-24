from structure.app.compiler.frontend.api import CompileTransform


class FrontendEndpoint:

    def compile(self) -> CompileTransform:
        return CompileTransform()
