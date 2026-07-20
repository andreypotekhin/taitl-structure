from structure.core.compiler.frontend.commands.CompilePlatformTransform import CompilePlatformTransform
from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform


class Frontend:
    def analyze(self) -> CompileTransform:
        return CompileTransform()

    def compile(self) -> CompilePlatformTransform:
        return CompilePlatformTransform()
