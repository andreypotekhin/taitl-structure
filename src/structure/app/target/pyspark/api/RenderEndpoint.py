from structure.app.target.pyspark.logic.actions.RenderPySparkExpression import RenderPySparkExpression
from structure.app.target.pyspark.logic.actions.RenderPySparkProject import RenderPySparkProject
from structure.app.target.pyspark.logic.actions.RenderPySparkRuntimeModule import RenderPySparkRuntimeModule
from structure.app.target.pyspark.logic.actions.RenderPySparkStep import RenderPySparkStep
from structure.app.target.pyspark.logic.actions.RenderPySparkTransformModule import RenderPySparkTransformModule


class RenderEndpoint:

    def expression(self) -> RenderPySparkExpression:
        return RenderPySparkExpression()

    def project(self) -> RenderPySparkProject:
        return RenderPySparkProject()

    def runtime(self) -> RenderPySparkRuntimeModule:
        return RenderPySparkRuntimeModule()

    def step(self) -> RenderPySparkStep:
        return RenderPySparkStep()

    def transform(self) -> RenderPySparkTransformModule:
        return RenderPySparkTransformModule()
