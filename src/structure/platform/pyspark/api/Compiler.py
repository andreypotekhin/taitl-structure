from structure.platform.api.v1 import CompilerAPI, CompileRequest, PlatformCompilation
from structure.platform.api.v1.model import TransformPlan
from structure.platform.pyspark.api.Authoring import PySparkStepBody
from structure.platform.pyspark.api.PySpark import PySpark


class Compiler(CompilerAPI):
    def compile(self, request: CompileRequest) -> PlatformCompilation:
        options = request.configuration
        plan = request.analysis
        if not isinstance(plan, TransformPlan):
            raise ValueError("PLATFORM-E2708: PySpark compilation requires a Core TransformPlan analysis.")
        if any(not isinstance(step.platform_body, PySparkStepBody) for step in plan.steps):
            raise ValueError("PLATFORM-E2708: PySpark compilation requires a PySpark-owned body for every step.")
        capabilities = PySpark.capabilities.resolve()(
            profile=str(options.get("profile", "")), variant=str(options.get("variant", ""))
        )
        lowered = PySpark.compiler.lower()(plan, capabilities=capabilities)
        schemas = (
            PySpark.schema.build()(lowered, types=options.get("schema_types"))
            if options.get("materialize_schemas", True)
            else None
        )
        return PlatformCompilation(lowered=lowered, fingerprint=plan.name, schemas=schemas)
