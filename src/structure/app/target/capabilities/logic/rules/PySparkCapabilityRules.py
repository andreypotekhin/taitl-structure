from structure.app.target.capabilities.model.BackendCapabilityError import BackendCapabilityError
from structure.app.target.capabilities.model.BackendId import BackendId
from structure.app.target.capabilities.model.CapabilityDecision import CapabilityDecision
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.capabilities.model.GeneratedImports import GeneratedImports

DEFAULT_TARGET_PROFILE = ">=3.5,<4.1"
DEFAULT_TARGET_VARIANT = "ordinary"

SUPPORTED_PROFILES = frozenset(
    {
        ">=3.5,<4.1",
        ">=3.5,<4.0",
        ">=4.0,<4.1",
    }
)

SUPPORTED_VARIANTS = frozenset({"ordinary", "spark-connect"})
PYSPARK_4_CAPABILITIES = frozenset({("expression", "try_cast")})

VARIANT_FAMILIES = {
    "ordinary": "ordinary_pyspark",
    "spark-connect": "spark_connect_dataframe",
}

COMMON_CAPABILITIES = frozenset(
    {
        ("expression", "field_ref"),
        ("expression", "literal"),
        ("expression", "projection"),
        ("expression", "filter"),
        ("expression", "boolean_ops"),
        ("expression", "equality"),
        ("expression", "null_safe_equality"),
        ("expression", "cast"),
        ("expression", "standard_helper_call"),
        ("join", "lookup_join"),
        ("join", "exists"),
        ("join", "not_exists"),
        ("join", "rowset_join"),
        ("join", "lookup_dedupe"),
        ("join", "temporal_one"),
        ("join", "as_of_one"),
        ("join", "left_join"),
        ("join", "inner_join"),
        ("join", "right_join"),
        ("join", "full_join"),
        ("join", "cross_join"),
        ("join", "left_semi_join"),
        ("join", "left_anti_join"),
        ("join", "composite_equi_join"),
        ("join", "non_equi_condition"),
        ("join", "disjunctive_condition"),
        ("join", "broadcast_hint"),
        ("join", "strategy_broadcast"),
        ("join", "strategy_shuffle_hash"),
        ("join", "strategy_merge"),
        ("join", "strategy_shuffle_replicate_nl"),
        ("aggregate", "group_by"),
        ("aggregate", "grouping_sets"),
        ("aggregate", "having"),
        ("aggregate", "rollup"),
        ("aggregate", "cube"),
        ("aggregate", "count"),
        ("aggregate", "count_distinct"),
        ("aggregate", "sum"),
        ("aggregate", "min"),
        ("aggregate", "max"),
        ("aggregate", "avg"),
        ("aggregate", "approx_count_distinct"),
        ("aggregate", "approx_percentile"),
        ("aggregate", "bool_and"),
        ("aggregate", "bool_or"),
        ("aggregate", "collect_list"),
        ("aggregate", "collect_set"),
        ("aggregate", "corr"),
        ("aggregate", "covar"),
        ("aggregate", "filtered_metric"),
        ("aggregate", "first"),
        ("aggregate", "first_value"),
        ("aggregate", "grouping_id"),
        ("aggregate", "is_grouped"),
        ("aggregate", "last_value"),
        ("aggregate", "stddev"),
        ("aggregate", "variance"),
        ("higher_order", "array_aggregate"),
        ("higher_order", "array_distinct"),
        ("higher_order", "array_exists"),
        ("higher_order", "array_filter"),
        ("higher_order", "array_flatten"),
        ("higher_order", "array_forall"),
        ("higher_order", "array_position"),
        ("higher_order", "array_sort_by"),
        ("higher_order", "array_transform"),
        ("higher_order", "array_zip_with"),
        ("higher_order", "map_entries"),
        ("higher_order", "map_filter"),
        ("higher_order", "map_from_entries"),
        ("higher_order", "map_keys"),
        ("higher_order", "map_transform_keys"),
        ("higher_order", "map_transform_values"),
        ("higher_order", "map_values"),
        ("higher_order", "map_zip_with"),
        ("dedupe", "drop_duplicates"),
        ("window", "avg"),
        ("window", "count"),
        ("window", "count_distinct"),
        ("window", "cume_dist"),
        ("window", "dense_rank"),
        ("window", "first_value"),
        ("window", "lag"),
        ("window", "last_value"),
        ("window", "lead"),
        ("window", "max"),
        ("window", "min"),
        ("window", "nth_value"),
        ("window", "ntile"),
        ("window", "percent_rank"),
        ("window", "rank"),
        ("window", "row_number"),
        ("window", "sum"),
        ("window", "rolling_avg"),
        ("window", "rolling_max"),
        ("window", "rolling_min"),
        ("window", "rolling_sum"),
        ("window", "select_latest"),
        ("window", "select_earliest"),
        ("optimization", "cache"),
        ("validation", "schema_only_validation"),
        ("validation", "strict_projection"),
        ("validation", "allow_extra_projection"),
        ("streaming", "row_local_projection"),
        ("streaming", "row_local_filter"),
        ("streaming", "watermark"),
        ("streaming", "stream_static_left_join"),
        ("streaming", "stream_static_inner_join"),
        ("imports", "generated_pyspark_imports"),
    }
)

ORDINARY_ONLY_CAPABILITIES = frozenset(
    {
        ("backend", "ordinary_pyspark"),
        ("backend", "spark_context"),
        ("backend", "rdd_access"),
        ("backend", "jvm_access"),
        ("backend", "private_classic_fields"),
        ("expression", "python_udf"),
    }
)

SPARK_CONNECT_ONLY_CAPABILITIES = frozenset({("backend", "spark_connect_dataframe")})

VARIANT_CAPABILITIES = {
    "ordinary": COMMON_CAPABILITIES | ORDINARY_ONLY_CAPABILITIES,
    "spark-connect": COMMON_CAPABILITIES | SPARK_CONNECT_ONLY_CAPABILITIES,
}

V1_CAPABILITIES = VARIANT_CAPABILITIES[DEFAULT_TARGET_VARIANT]


class PySparkCapabilities:

    def __init__(
        self,
        *,
        target_profile: str = DEFAULT_TARGET_PROFILE,
        target_variant: str = DEFAULT_TARGET_VARIANT,
        supported: frozenset[tuple[str, str]] | None = None,
    ) -> None:
        family = VARIANT_FAMILIES.get(target_variant, "unknown")
        self.id = BackendId(name="pyspark", target=target_profile, family=family, variant=target_variant)
        base_capabilities = (
            supported if supported is not None else VARIANT_CAPABILITIES.get(target_variant, COMMON_CAPABILITIES)
        )
        self.supported = (
            base_capabilities | PYSPARK_4_CAPABILITIES
            if supported is None and target_profile == ">=4.0,<4.1"
            else base_capabilities
        )
        self._imports = GeneratedImports()

    def imports(self) -> GeneratedImports:
        return self._imports

    def supports(self, requirement: CapabilityRequirement) -> CapabilityDecision:
        if self.id.target not in SUPPORTED_PROFILES:
            return CapabilityDecision.unsupported_capability(
                backend=self.id,
                requirement=requirement,
                rationale="No static PySpark capability profile exists for the configured target range.",
                use=f"Set target_profile = {DEFAULT_TARGET_PROFILE!r}.",
                required_target=DEFAULT_TARGET_PROFILE,
            )

        if self.id.variant not in SUPPORTED_VARIANTS:
            return CapabilityDecision.unsupported_capability(
                backend=self.id,
                requirement=requirement,
                rationale="No PySpark capability profile exists for the configured target variant.",
                use='Set target_variant = "ordinary" or target_variant = "spark-connect".',
                required_target=DEFAULT_TARGET_VARIANT,
            )

        if requirement.key() in self.supported:
            return CapabilityDecision.ok(backend=self.id, requirement=requirement)

        return CapabilityDecision.unsupported_capability(
            backend=self.id,
            requirement=requirement,
            rationale=(
                "The feature is not part of the v1 PySpark capability profile. Keeping it unsupported prevents "
                "silent fallback to opaque Spark or Python behavior."
            ),
            use="Use a supported v1 Structure operation or an explicit hook, or wait for the feature's specification.",
        )

    def require(self, requirement: CapabilityRequirement) -> CapabilityDecision:
        decision = self.supports(requirement)
        if not decision.supported:
            raise BackendCapabilityError(decision)
        return decision
