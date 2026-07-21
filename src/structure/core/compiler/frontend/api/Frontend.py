from structure.core.compiler.frontend.commands.AnalyzeTransform import AnalyzeTransform
from structure.core.compiler.frontend.commands.CompilePluginTransform import CompilePluginTransform
from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform


class Frontend:
    def analyze(self) -> AnalyzeTransform:
        return AnalyzeTransform()

    def author(self) -> CompileTransform:
        """Author target bodies for the integrated bundled-plugin documentation workflow."""
        return CompileTransform()

    def compile(self) -> CompilePluginTransform:
        return CompilePluginTransform()
