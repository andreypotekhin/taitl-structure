from structure.app.cli.logic.actions.DiscoverStructureProject import DiscoverStructureProject
from structure.app.cli.logic.actions.RenderConfiguredPySparkProject import RenderConfiguredPySparkProject
from structure.app.cli.logic.actions.RenderExplainReport import RenderExplainReport


class CliActionsEndpoint:

    def discover_project(self) -> DiscoverStructureProject:
        return DiscoverStructureProject()

    def render_configured_pyspark_project(self) -> RenderConfiguredPySparkProject:
        return RenderConfiguredPySparkProject()

    def render_explain_report(self) -> RenderExplainReport:
        return RenderExplainReport()
