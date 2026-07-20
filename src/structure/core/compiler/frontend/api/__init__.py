from structure.core.compiler.frontend.api.Frontend import Frontend
from structure.core.compiler.frontend.commands.CompilePlatformTransform import CompilePlatformTransform
from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform


compile_transform = Frontend().analyze()

__all__ = [
    "CompilePlatformTransform",
    "CompileTransform",
    "Frontend",
    "compile_transform",
]
