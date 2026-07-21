from structure.core.compiler.frontend.api.Frontend import Frontend
from structure.core.compiler.frontend.commands.AnalyzeTransform import AnalyzeTransform
from structure.core.compiler.frontend.commands.CompilePluginTransform import CompilePluginTransform
from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform


# Transitional compatibility while callers migrate to `Compiler.frontend.analyze()`
# or `Compiler.frontend.compile()`. P072 removes this alias with its Core PySpark shims.
compile_transform = CompileTransform()

__all__ = [
    "CompilePluginTransform",
    "CompileTransform",
    "Frontend",
    "AnalyzeTransform",
    "compile_transform",
]
