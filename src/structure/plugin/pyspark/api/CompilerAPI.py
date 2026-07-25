from structure.plugin.api.v1 import CompilationPurpose
from structure.plugin.api.v1 import CompilerAPI as CompilerAPIV1
from structure.plugin.api.v1 import CompileRequest, PluginCompilation
from structure.plugin.api.v1.model import TransformPlan
from structure.plugin.pyspark.api.AuthoringAPI import PySparkStepBody
from structure.plugin.pyspark.api.PySpark import PySpark


class CompilerAPI(CompilerAPIV1):
    def __init__(self) -> None:
        self._udf_diagnostics = PySpark.compiler.udf_diagnostics()

    def compile(self, request: CompileRequest) -> PluginCompilation:
        options = request.configuration
        plan = request.analysis
        if not isinstance(plan, TransformPlan):
            raise ValueError("PLUGIN-E2708: PySpark compilation requires a Core TransformPlan analysis.")
        if any(not isinstance(step.plugin_body, PySparkStepBody) for step in plan.steps):
            raise ValueError("PLUGIN-E2708: PySpark compilation requires a PySpark-owned body for every step.")
        PySpark.compiler.hooks()(plan)
        if request.purpose is CompilationPurpose.DOCUMENTATION:
            return PluginCompilation(
                lowered=None,
                fingerprint=plan.name,
                diagnostics=self._udf_diagnostics(plan, enabled=bool(options.get("warn_on_udfs", True))),
            )
        plugin_options = request.plugin_options
        capabilities = PySpark.capabilities.resolve()(
            profile=str(plugin_options.get("profile", "")), variant=str(plugin_options.get("variant", ""))
        )
        lowered = PySpark.compiler.lower()(
            plan,
            capabilities=capabilities,
            check_intermediate=bool(options.get("validate_intermediate", True)),
        )
        schemas = (
            PySpark.schema.build()(lowered, types=options.get("schema_types"))
            if options.get("materialize_schemas", True)
            else None
        )
        return PluginCompilation(
            lowered=lowered,
            fingerprint=plan.name,
            schemas=schemas,
            diagnostics=self._udf_diagnostics(plan, enabled=bool(options.get("warn_on_udfs", True))),
        )
