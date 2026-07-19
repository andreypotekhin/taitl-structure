from structure.platform.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.platform.pyspark.compiler.commands.BuildCompilerTraceability import BuildCompilerTraceability
from structure.platform.pyspark.compiler.commands.ClassifyStreamingCompatibility import ClassifyStreamingCompatibility
from structure.platform.pyspark.compiler.commands.LowerPySparkPlan import LowerPySparkPlan


class Compiler:

    def lower(self) -> LowerPySparkPlan:
        return LowerPySparkPlan(PySparkCapabilities())

    def streaming(self) -> ClassifyStreamingCompatibility:
        return ClassifyStreamingCompatibility()

    def traceability(self) -> BuildCompilerTraceability:
        return BuildCompilerTraceability()
