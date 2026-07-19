from structure.platform.pyspark.commands.RenderPySparkExpression import RenderPySparkExpression
from structure.platform.pyspark.commands.RenderPySparkProject import RenderPySparkProject
from structure.platform.pyspark.commands.RenderPySparkRuntimeModule import RenderPySparkRuntimeModule
from structure.platform.pyspark.commands.RenderPySparkStep import RenderPySparkStep
from structure.platform.pyspark.commands.RenderPySparkTransformModule import RenderPySparkTransformModule


class Render:

    @staticmethod
    def expression() -> RenderPySparkExpression:
        return RenderPySparkExpression()

    @staticmethod
    def project() -> RenderPySparkProject:
        return RenderPySparkProject()

    @staticmethod
    def runtime() -> RenderPySparkRuntimeModule:
        return RenderPySparkRuntimeModule()

    @staticmethod
    def step() -> RenderPySparkStep:
        return RenderPySparkStep()

    @staticmethod
    def transform() -> RenderPySparkTransformModule:
        return RenderPySparkTransformModule()
