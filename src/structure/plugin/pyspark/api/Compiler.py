from structure.plugin.api.v1 import CompilerAPI, CompileRequest, PluginCompilation
from structure.plugin.api.v1.model import TransformPlan
from structure.plugin.pyspark.api.Authoring import PySparkStepBody
from structure.plugin.pyspark.api.PySpark import PySpark


class Compiler(CompilerAPI):
    def compile(self, request: CompileRequest) -> PluginCompilation:
        options = request.configuration
        plan = request.analysis
        if not isinstance(plan, TransformPlan):
            raise ValueError("PLUGIN-E2708: PySpark compilation requires a Core TransformPlan analysis.")
        if any(not isinstance(step.plugin_body, PySparkStepBody) for step in plan.steps):
            raise ValueError("PLUGIN-E2708: PySpark compilation requires a PySpark-owned body for every step.")
        capabilities = PySpark.capabilities.resolve()(
            profile=str(options.get("profile", "")), variant=str(options.get("variant", ""))
        )
        lowered = PySpark.compiler.lower()(plan, capabilities=capabilities)
        schemas = (
            PySpark.schema.build()(lowered, types=options.get("schema_types"))
            if options.get("materialize_schemas", True)
            else None
        )
        return PluginCompilation(lowered=lowered, fingerprint=plan.name, schemas=schemas)
