from structure.plugin.api.v1.model import (
    BackendCapabilityError,
    BackendId,
    CapabilityDecision,
    CapabilityRequirement,
    GeneratedImports,
)

DEFAULT_TARGET_PROFILE = ">=3.5,<4.1"
DEFAULT_TARGET_VARIANT = "ordinary"

SUPPORTED_PROFILES = frozenset(
    {
        ">=3.5,<4.1",
        ">=3.5,<4.0",
        ">=4.0,<4.1",
        ">=4.2,<4.3",
    }
)

SUPPORTED_VARIANTS = frozenset({"ordinary", "spark-connect"})
PYSPARK_4_CAPABILITIES = frozenset(
    {
        ("aggregate", "schema_of_variant_agg"),
        ("generator", "variant_explode"),
        ("generator", "variant_explode_outer"),
        ("expression", "try_cast"),
        ("expression", "variant"),
        ("schema", "variant"),
    }
)
PYSPARK_4_2_CAPABILITIES = frozenset({("expression", "is_valid_variant")})
MATERIALIZATION_CAPABILITIES = frozenset(
    {
        ("optimization", "persist"),
        ("optimization", "unpersist"),
        ("optimization", "checkpoint"),
        ("optimization", "local_checkpoint"),
    }
)
MATERIALIZATION_PROFILES = frozenset({">=3.5,<4.1", ">=3.5,<4.0", ">=4.0,<4.1"})
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
        ("expression", "bitwise"),
        ("expression", "equality"),
        ("expression", "null_safe_equality"),
        ("expression", "cast"),
        ("expression", "python_udf"),
        ("expression", "rand"),
        ("relation", "exactly_one"),
        ("expression", "standard_helper_call"),
        ("pyspark", "ordered_timeline_scan"),
        ("geo", "geometry"),
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
        ("aggregate", "mode"),
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
        ("aggregate", "kurtosis"),
        ("aggregate", "percentile"),
        ("aggregate", "skewness"),
        ("aggregate", "stddev"),
        ("aggregate", "variance"),
        ("higher_order", "array_aggregate"),
        ("higher_order", "array"),
        ("higher_order", "array_append"),
        ("higher_order", "array_compact"),
        ("higher_order", "array_contains"),
        ("higher_order", "array_distinct"),
        ("higher_order", "array_except"),
        ("higher_order", "array_intersect"),
        ("higher_order", "array_exists"),
        ("higher_order", "array_filter"),
        ("higher_order", "array_flatten"),
        ("higher_order", "array_forall"),
        ("higher_order", "array_position"),
        ("higher_order", "array_prepend"),
        ("higher_order", "array_reverse"),
        ("higher_order", "array_insert"),
        ("higher_order", "array_remove"),
        ("higher_order", "array_sequence"),
        ("higher_order", "array_sort"),
        ("higher_order", "array_sort_by"),
        ("higher_order", "array_transform"),
        ("higher_order", "array_repeat"),
        ("higher_order", "array_slice"),
        ("higher_order", "array_union"),
        ("higher_order", "collection_size"),
        ("higher_order", "element_at"),
        ("higher_order", "array_zip_with"),
        ("higher_order", "map_entries"),
        ("higher_order", "map_concat"),
        ("higher_order", "map_contains_key"),
        ("higher_order", "map_filter"),
        ("higher_order", "map_from_entries"),
        ("higher_order", "map_keys"),
        ("higher_order", "map_transform_keys"),
        ("higher_order", "map_transform_values"),
        ("higher_order", "map_values"),
        ("higher_order", "map_zip_with"),
        ("higher_order", "try_element_at"),
        ("dedupe", "drop_duplicates"),
        ("generator", "explode_struct"),
        ("generator", "explode_outer_struct"),
        ("generator", "inline_struct"),
        ("generator", "inline_outer_struct"),
        ("generator", "posexplode_outer_struct"),
        ("generator", "posexplode_struct"),
        ("generator", "explode_array"),
        ("generator", "explode_outer_array"),
        ("generator", "posexplode_array"),
        ("generator", "posexplode_outer_array"),
        ("generator", "explode_map"),
        ("generator", "explode_outer_map"),
        ("generator", "posexplode_map"),
        ("generator", "posexplode_outer_map"),
        ("relation", "hierarchy_closure"),
        ("relation", "hierarchy_fallbacks"),
        ("relation", "limit"),
        ("relation", "offset"),
        ("relation", "order_by"),
        ("relation", "relation_alias"),
        ("relation", "require_all"),
        ("relation", "require_parent_hierarchy"),
        ("relation", "require_reference"),
        ("relation", "require_unique"),
        ("relation", "sample"),
        ("relation", "select_first_qualified"),
        ("set", "except_all"),
        ("set", "intersect"),
        ("set", "intersect_all"),
        ("set", "subtract"),
        ("set", "union_all"),
        ("set", "union_by_name"),
        ("window", "avg"),
        ("window", "bool_and"),
        ("window", "bool_or"),
        ("window", "collect_list"),
        ("window", "collect_set"),
        ("window", "count"),
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
        ("window", "stddev"),
        ("window", "variance"),
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
        ("streaming", "time_window"),
        ("streaming", "window_time"),
        ("streaming", "session_window"),
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
        ("streaming", "session_window_aggregate"),
        ("streaming", "stream_static_left_semi_join"),
        ("streaming", "stream_stream_outer_join"),
        ("streaming", "stream_stream_left_semi_join"),
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
        self.supported = self._supported(base_capabilities, target_profile=target_profile, explicit=supported)
        self._imports = GeneratedImports()

    def imports(self) -> GeneratedImports:
        return self._imports

    def _supported(
        self,
        base_capabilities: frozenset[tuple[str, str]],
        *,
        target_profile: str,
        explicit: frozenset[tuple[str, str]] | None,
    ) -> frozenset[tuple[str, str]]:
        if explicit is not None:
            return base_capabilities
        if target_profile == ">=4.0,<4.1":
            base_capabilities = base_capabilities | PYSPARK_4_CAPABILITIES
        elif target_profile == ">=4.2,<4.3":
            base_capabilities = base_capabilities | PYSPARK_4_CAPABILITIES | PYSPARK_4_2_CAPABILITIES
        if self.id.variant == "ordinary" and target_profile in MATERIALIZATION_PROFILES:
            base_capabilities |= MATERIALIZATION_CAPABILITIES
        return base_capabilities

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
