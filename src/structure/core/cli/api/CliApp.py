from structure.core.cli.commands.CheckStructureProject import CheckStructureProject
from structure.core.cli.commands.CleanGeneratedFiles import CleanGeneratedFiles
from structure.core.cli.commands.CompileStructureProject import CompileStructureProject
from structure.core.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.core.cli.commands.ExplainStructureTransform import ExplainStructureTransform
from structure.core.cli.commands.RenderConfiguredPySparkProject import RenderConfiguredPySparkProject
from structure.core.cli.commands.RenderExplainReport import RenderExplainReport
from structure.core.cli.commands.ResolveCliConfig import ResolveCliConfig
from structure.core.cli.commands.WriteStructureConfig import WriteStructureConfig


class CliApp:

    @staticmethod
    def resolve_config() -> ResolveCliConfig:
        return ResolveCliConfig()

    @staticmethod
    def write_config() -> WriteStructureConfig:
        return WriteStructureConfig()

    @staticmethod
    def check_project() -> CheckStructureProject:
        return CheckStructureProject()

    @staticmethod
    def compile_project() -> CompileStructureProject:
        return CompileStructureProject()

    @staticmethod
    def explain_transform() -> ExplainStructureTransform:
        return ExplainStructureTransform()

    @staticmethod
    def clean_generated_files() -> CleanGeneratedFiles:
        return CleanGeneratedFiles()

    @staticmethod
    def discover_project() -> DiscoverStructureProject:
        return DiscoverStructureProject()

    @staticmethod
    def render_configured_pyspark_project() -> RenderConfiguredPySparkProject:
        return RenderConfiguredPySparkProject()

    @staticmethod
    def render_explain_report() -> RenderExplainReport:
        return RenderExplainReport()
