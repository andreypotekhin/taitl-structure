from structure.app.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.app.cli.commands.RenderConfiguredPySparkProject import RenderConfiguredPySparkProject
from structure.app.cli.commands.RenderExplainReport import RenderExplainReport


class CliActions:

    @staticmethod
    def discover_project() -> DiscoverStructureProject:
        return DiscoverStructureProject()

    @staticmethod
    def render_configured_pyspark_project() -> RenderConfiguredPySparkProject:
        return RenderConfiguredPySparkProject()

    @staticmethod
    def render_explain_report() -> RenderExplainReport:
        return RenderExplainReport()
