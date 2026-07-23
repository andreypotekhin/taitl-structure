from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.commands.BuildCompilerTraceability import BuildCompilerTraceability
from structure.plugin.pyspark.compiler.commands.BuildPySparkUdfDiagnostics import BuildPySparkUdfDiagnostics
from structure.plugin.pyspark.compiler.commands.ClassifyStreamingCompatibility import ClassifyStreamingCompatibility
from structure.plugin.pyspark.compiler.commands.LowerPySparkPlan import LowerPySparkPlan
from structure.plugin.pyspark.compiler.commands.ValidatePySparkHooks import ValidatePySparkHooks


class Compiler:

    def lower(self) -> LowerPySparkPlan:
        return LowerPySparkPlan(PySparkCapabilities())

    def streaming(self) -> ClassifyStreamingCompatibility:
        return ClassifyStreamingCompatibility()

    def traceability(self) -> BuildCompilerTraceability:
        return BuildCompilerTraceability()

    def udf_diagnostics(self) -> BuildPySparkUdfDiagnostics:
        return BuildPySparkUdfDiagnostics()

    def hooks(self) -> ValidatePySparkHooks:
        return ValidatePySparkHooks()
