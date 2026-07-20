from structure.platform.pyspark.render.commands.DescribePySparkDocumentation import DescribePySparkDocumentation
from structure.platform.pyspark.render.commands.RenderPySparkExplainReport import RenderPySparkExplainReport
from structure.platform.pyspark.render.commands.RenderPySparkExpression import RenderPySparkExpression
from structure.platform.pyspark.render.commands.RenderPySparkProject import RenderPySparkProject
from structure.platform.pyspark.render.commands.RenderPySparkRuntimeModule import RenderPySparkRuntimeModule
from structure.platform.pyspark.render.commands.RenderPySparkStep import RenderPySparkStep
from structure.platform.pyspark.render.commands.RenderPySparkTransformModule import RenderPySparkTransformModule
from structure.platform.pyspark.render.logic.GeneratedCodeOptions import GeneratedCodeOptions


class Render:

    def documentation(self) -> DescribePySparkDocumentation:
        return DescribePySparkDocumentation()

    def expression(self) -> RenderPySparkExpression:
        return RenderPySparkExpression()

    def options(self) -> GeneratedCodeOptions:
        return GeneratedCodeOptions()

    def explain(self) -> RenderPySparkExplainReport:
        return RenderPySparkExplainReport()

    def project(self) -> RenderPySparkProject:
        return RenderPySparkProject()

    def runtime(self) -> RenderPySparkRuntimeModule:
        return RenderPySparkRuntimeModule()

    def step(self) -> RenderPySparkStep:
        return RenderPySparkStep()

    def transform(self) -> RenderPySparkTransformModule:
        return RenderPySparkTransformModule()
