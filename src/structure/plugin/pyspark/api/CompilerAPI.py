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
        warn_on_udfs = self._warn_on_udfs(plan, default=bool(options.get("warn_on_udfs", True)))
        if request.purpose is CompilationPurpose.DOCUMENTATION:
            return PluginCompilation(
                lowered=None,
                fingerprint=plan.name,
                diagnostics=self._udf_diagnostics(plan, enabled=warn_on_udfs),
            )
        plugin_options = request.plugin_options
        capabilities = PySpark.capabilities.resolve()(
            profile=str(plugin_options.get("profile", "")), variant=str(plugin_options.get("variant", ""))
        )
        boundary_policy = self._boundary_policy(plugin_options, capabilities.id.variant)
        check_intermediate = bool(
            (plan.options or {}).get("validate_intermediate", options.get("validate_intermediate", True))
        )
        lowered = PySpark.compiler.lower()(
            plan,
            capabilities=capabilities,
            check_intermediate=check_intermediate,
            boundary_policy=boundary_policy,
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
            diagnostics=self._udf_diagnostics(plan, enabled=warn_on_udfs),
        )

    @staticmethod
    def _warn_on_udfs(plan: TransformPlan, *, default: bool) -> bool:
        return bool((plan.options or {}).get("warn_on_udfs", default))

    @staticmethod
    def _boundary_policy(options, variant: str) -> str:
        default = "auto" if variant == "spark-connect" else "off"
        policy = str(options.get("connect_plan_boundaries", default))
        if policy not in {"off", "auto", "strict"}:
            raise ValueError(
                "PLUGIN-E2710: connect_plan_boundaries must be one of 'off', 'auto', or 'strict'."
            )
        return policy if variant == "spark-connect" else "off"
