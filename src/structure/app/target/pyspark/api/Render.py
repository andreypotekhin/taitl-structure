from structure.app.target.pyspark.commands.RenderPySparkExpression import RenderPySparkExpression
from structure.app.target.pyspark.commands.RenderPySparkProject import RenderPySparkProject
from structure.app.target.pyspark.commands.RenderPySparkRuntimeModule import RenderPySparkRuntimeModule
from structure.app.target.pyspark.commands.RenderPySparkStep import RenderPySparkStep
from structure.app.target.pyspark.commands.RenderPySparkTransformModule import RenderPySparkTransformModule


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
