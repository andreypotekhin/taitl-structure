from structure.core.compiler.frontend.commands.CompilePlatformTransform import CompilePlatformTransform
from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform


class Frontend:

    @staticmethod
    def analyze() -> CompileTransform:
        return CompileTransform()

    @staticmethod
    def compile() -> CompilePlatformTransform:
        return CompilePlatformTransform()
