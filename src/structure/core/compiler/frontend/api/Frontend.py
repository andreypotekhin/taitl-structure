from structure.core.compiler.frontend.commands.AnalyzeTransform import AnalyzeTransform
from structure.core.compiler.frontend.commands.CompilePluginTransform import CompilePluginTransform


class Frontend:
    def analyze(self) -> AnalyzeTransform:
        return AnalyzeTransform()

    def compile(self) -> CompilePluginTransform:
        return CompilePluginTransform()
