from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.commands.BuildCompilerTraceability import BuildCompilerTraceability
from structure.plugin.pyspark.compiler.commands.BuildPySparkExpressionDiagnostics import (
    BuildPySparkExpressionDiagnostics,
)
from structure.plugin.pyspark.compiler.commands.BuildPySparkLineageDiagnostics import BuildPySparkLineageDiagnostics
from structure.plugin.pyspark.compiler.commands.BuildPySparkUdfDiagnostics import BuildPySparkUdfDiagnostics
from structure.plugin.pyspark.compiler.commands.ClassifyStreamingCompatibility import ClassifyStreamingCompatibility
from structure.plugin.pyspark.compiler.commands.DescribePySparkOptimization import DescribePySparkOptimization
from structure.plugin.pyspark.compiler.commands.LowerPySparkPlan import LowerPySparkPlan
from structure.plugin.pyspark.compiler.commands.OptimizePySparkProjectionUnions import OptimizePySparkProjectionUnions
from structure.plugin.pyspark.compiler.commands.ValidatePySparkHooks import ValidatePySparkHooks


class Compiler:

    def optimization_graph(self) -> DescribePySparkOptimization:
        return DescribePySparkOptimization()

    def lower(self) -> LowerPySparkPlan:
        return LowerPySparkPlan(PySparkCapabilities())

    def optimize_projection_unions(self) -> OptimizePySparkProjectionUnions:
        return OptimizePySparkProjectionUnions()

    def streaming(self) -> ClassifyStreamingCompatibility:
        return ClassifyStreamingCompatibility()

    def traceability(self) -> BuildCompilerTraceability:
        return BuildCompilerTraceability()

    def udf_diagnostics(self) -> BuildPySparkUdfDiagnostics:
        return BuildPySparkUdfDiagnostics()

    def expression_diagnostics(self) -> BuildPySparkExpressionDiagnostics:
        return BuildPySparkExpressionDiagnostics()

    def lineage_diagnostics(self) -> BuildPySparkLineageDiagnostics:
        return BuildPySparkLineageDiagnostics()

    def hooks(self) -> ValidatePySparkHooks:
        return ValidatePySparkHooks()
