from structure.core.compiler.frontend.api import CompileTransform


class Frontend:

    @staticmethod
    def compile() -> CompileTransform:
        return CompileTransform()
