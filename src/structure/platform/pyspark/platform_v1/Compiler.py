from typing import Any, cast

from structure.core.compiler.api import Compiler as CoreCompiler
from structure.platform.api.v1 import CompilerAPI, CompileRequest, PlatformCompilation
from structure.platform.pyspark.api import PySpark
from structure.platform.pyspark.capabilities.PySparkCapabilityRules import PySparkCapabilities
from structure.platform.pyspark.schemas.BuildTransformSchemas import BuildTransformSchemas


class Compiler(CompilerAPI):
    def compile(self, request: CompileRequest) -> PlatformCompilation:
        options = request.configuration
        plan = CoreCompiler.frontend.compile()(
            cast(Any, request.transform),
            warn_on_udfs=bool(options.get("warn_on_udfs", False)),
            generated_code_options=cast(tuple[str, ...], options.get("generated_code_options", ())),
        )
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
        return PlatformCompilation(lowered=lowered, fingerprint=plan.name, analysis=plan, schemas=schemas)
