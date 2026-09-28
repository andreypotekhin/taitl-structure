from dataclasses import replace

from structure.plugin.api.v1 import OptimizationReport, OptimizationRequest, PluginCompilation, TransformPlan
from structure.plugin.pyspark.api.CompilerAPI import CompilerAPI
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class OptimizationAPI:
    def describe(self, request: OptimizationRequest):
        return PySpark.compiler.optimization_graph()(self._plan(request))

    def apply(self, request: OptimizationRequest, selection: OptimizationReport) -> PluginCompilation:
        plan = self._plan(request)
        retained = frozenset(selection.retained_steps)
        lowered = replace(plan, steps=tuple(step for step in plan.steps if step.name in retained), pruning=selection)
        compile_request = request.compile_request
        original = compile_request.analysis
        if not isinstance(original, TransformPlan):
            raise ValueError("PLUGIN-E2708: PySpark optimization requires authored TransformPlan analysis.")
        compiler = CompilerAPI()
        options = compile_request.configuration
        diagnostics = compiler._diagnostics(
            original,
            warn_on_udfs=False,
            warn_on_lineage_growth=compiler._warn_on_lineage_growth(
                original, default=bool(options.get("warn_on_lineage_growth", True))
            ),
            execution_plan=lowered,
            **compiler._lineage_thresholds(compile_request.plugin_options),
        )
        growth_codes = {"PYSPARK-W2701", "PYSPARK-W2702", "PYSPARK-W2703", "PYSPARK-W2704"}
        diagnostics = (
            *(item for item in request.compilation.diagnostics if getattr(item, "code", None) not in growth_codes),
            *diagnostics,
        )
        return replace(
            request.compilation,
            lowered=lowered,
            executable_steps=selection.retained_steps,
            diagnostics=diagnostics,
        )

    @staticmethod
    def _plan(request: OptimizationRequest) -> PySparkExecutionPlan:
        plan = request.compilation.lowered
        if not isinstance(plan, PySparkExecutionPlan):
            raise ValueError("PLUGIN-E2708: PySpark optimization requires a lowered PySpark plan.")
        return plan
