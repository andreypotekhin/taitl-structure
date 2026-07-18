from structure.core.target.pyspark.commands.RenderPySparkExpression import RenderPySparkExpression
from structure.core.target.pyspark.commands.RenderPySparkProject import RenderPySparkProject
from structure.core.target.pyspark.commands.RenderPySparkRuntimeModule import RenderPySparkRuntimeModule
from structure.core.target.pyspark.commands.RenderPySparkStep import RenderPySparkStep
from structure.core.target.pyspark.commands.RenderPySparkTransformModule import RenderPySparkTransformModule


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
