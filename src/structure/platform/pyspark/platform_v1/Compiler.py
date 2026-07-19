from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.platform.api.v1 import CompilerAPI, CompileRequest, PlatformCompilation
from structure.platform.pyspark.api import PySpark
from structure.platform.pyspark.capabilities.PySparkCapabilityRules import PySparkCapabilities
from structure.platform.pyspark.schemas.BuildTransformSchemas import BuildTransformSchemas


class Compiler(CompilerAPI):
    def compile(self, request: CompileRequest) -> PlatformCompilation:
        options = request.configuration
        plan = request.analysis
        if not isinstance(plan, TransformPlan):
            raise ValueError("PLATFORM-E2708: PySpark compilation requires a Core TransformPlan analysis.")
        capabilities = PySparkCapabilities(
            target_profile=str(options.get("profile", "")),
            target_variant=str(options.get("variant", "")),
        )
        lowered = PySpark.plan.lower()(plan, capabilities=capabilities)
        schemas = (
            BuildTransformSchemas()(lowered, types=options.get("schema_types"))
            if options.get("materialize_schemas", True)
            else None
        )
        return PlatformCompilation(lowered=lowered, fingerprint=plan.name, schemas=schemas)
