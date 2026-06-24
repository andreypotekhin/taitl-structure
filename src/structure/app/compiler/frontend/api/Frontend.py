from structure.app.compiler.frontend.commands.CompileTransform import CompileTransform


class Frontend:

    @staticmethod
    def compile() -> CompileTransform:
        return CompileTransform()
