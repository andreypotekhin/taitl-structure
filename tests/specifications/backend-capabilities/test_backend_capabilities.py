import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from structure.core.target.capabilities.api import (
    BACKEND_E2401,
    BACKEND_E2402,
    BackendCapabilityError,
    Capabilities,
    CapabilityRequirement,
)
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities


def test_default_pyspark_capabilities_do_not_import_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    resolved = Capabilities.resolve()()

    assert resolved.id.name == "pyspark"
    assert resolved.id.target == ">=3.5,<4.1"
    assert resolved.id.variant == "ordinary"
    assert resolved.id.family == "ordinary_pyspark"
    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before


def test_v5_capabilities_accept_a_generic_target_name() -> None:
    resolved = Capabilities.resolve()(target="pyspark")

    assert resolved.id.name == "pyspark"


def test_supported_v1_requirement_passes() -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="join", name="lookup_join"))

    assert decision.supported
    assert decision.code == ""


@pytest.mark.parametrize("target_variant", ["ordinary", "spark-connect"])
def test_bitwise_expression_capability_is_available_on_each_batch_target(target_variant: str) -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": target_variant})

    assert resolved.require(CapabilityRequirement(group="expression", name="bitwise")).supported


@pytest.mark.parametrize("group, name", [("expression", "python_udf"), ("relation", "exactly_one")])
def test_spark_connect_accepts_public_batch_capabilities(group: str, name: str) -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": "spark-connect"})

    assert resolved.require(CapabilityRequirement(group=group, name=name)).supported


@pytest.mark.parametrize(
    "name",
    [
        "exists",
        "not_exists",
        "inner_join",
        "temporal_one",
        "as_of_one",
        "left_semi_join",
        "left_anti_join",
        "strategy_broadcast",
        "strategy_shuffle_hash",
        "strategy_merge",
        "strategy_shuffle_replicate_nl",
    ],
)
def test_supported_v2_join_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="join", name=name))

    assert decision.supported


@pytest.mark.parametrize(
    "name",
    [
        "approx_count_distinct",
        "approx_percentile",
        "avg",
        "bool_and",
        "bool_or",
        "collect_list",
        "collect_set",
        "corr",
        "count",
        "count_distinct",
        "covar",
        "cube",
        "filtered_metric",
        "first_value",
        "group_by",
        "grouping_id",
        "grouping_sets",
        "having",
        "is_grouped",
        "kurtosis",
        "last_value",
        "max",
        "min",
        "mode",
        "percentile",
        "rollup",
        "stddev",
        "skewness",
        "sum",
        "variance",
    ],
)
def test_supported_v2_aggregate_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="aggregate", name=name))

    assert decision.supported


def test_supported_session_window_requirement_passes() -> None:
    decision = Capabilities.resolve()().require(CapabilityRequirement(group="streaming", name="session_window"))

    assert decision.supported


@pytest.mark.parametrize(
    "name",
    [
        "session_window_aggregate",
        "stream_static_left_semi_join",
        "stream_stream_outer_join",
        "stream_stream_left_semi_join",
    ],
)
def test_v4_streaming_requirements_are_ordinary_pyspark_only(name: str) -> None:
    ordinary = Capabilities.resolve()().require(CapabilityRequirement(group="streaming", name=name))
    connect = Capabilities.resolve()(options={"variant": "spark-connect"}).supports(
        CapabilityRequirement(group="streaming", name=name)
    )

    assert ordinary.supported
    assert not connect.supported


@pytest.mark.parametrize(
    "name",
    [
        "array",
        "array_aggregate",
        "array_reduce",
        "array_append",
        "array_compact",
        "array_contains",
        "array_distinct",
        "array_except",
        "array_intersect",
        "array_exists",
        "array_filter",
        "array_flatten",
        "array_forall",
        "array_position",
        "array_prepend",
        "array_reverse",
        "array_insert",
        "array_remove",
        "array_sequence",
        "array_sort",
        "array_sort_by",
        "array_transform",
        "array_repeat",
        "array_slice",
        "array_union",
        "array_zip_with",
        "collection_size",
        "cardinality",
        "array_size",
        "array_max",
        "array_min",
        "array_join",
        "concat",
        "arrays_overlap",
        "get",
        "sort_array",
        "shuffle",
        "arrays_zip",
        "element_at",
        "map_concat",
        "map_contains_key",
        "map_entries",
        "map_filter",
        "map_from_entries",
        "map_keys",
        "map_transform_keys",
        "map_transform_values",
        "map_values",
        "map_zip_with",
        "try_element_at",
    ],
)
def test_supported_v2_higher_order_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="higher_order", name=name))

    assert decision.supported


@pytest.mark.parametrize("target_variant", ["ordinary", "spark-connect"])
@pytest.mark.parametrize(
    "name",
    [
        "array",
        "array_aggregate",
        "array_reduce",
        "array_append",
        "array_compact",
        "array_contains",
        "array_distinct",
        "array_except",
        "array_intersect",
        "array_exists",
        "array_filter",
        "array_flatten",
        "array_forall",
        "array_position",
        "array_repeat",
        "array_slice",
        "array_prepend",
        "array_reverse",
        "array_insert",
        "array_remove",
        "array_sequence",
        "array_sort",
        "array_sort_by",
        "array_transform",
        "array_union",
        "array_zip_with",
        "collection_size",
        "cardinality",
        "array_size",
        "array_max",
        "array_min",
        "array_join",
        "concat",
        "arrays_overlap",
        "get",
        "sort_array",
        "shuffle",
        "arrays_zip",
        "element_at",
        "map_concat",
        "map_contains_key",
        "map_entries",
        "map_filter",
        "map_from_entries",
        "map_keys",
        "map_transform_keys",
        "map_transform_values",
        "map_values",
        "map_zip_with",
        "try_element_at",
    ],
)
def test_v3_collection_capabilities_are_explicit_for_each_supported_variant(target_variant: str, name: str) -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": target_variant})

    assert resolved.require(CapabilityRequirement(group="higher_order", name=name)).supported


def test_supported_v2_dedupe_requirement_passes() -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="dedupe", name="drop_duplicates"))

    assert decision.supported


@pytest.mark.parametrize(
    "name",
    [
        "dense_rank",
        "avg",
        "bool_and",
        "bool_or",
        "collect_list",
        "collect_set",
        "count",
        "cume_dist",
        "first_value",
        "lag",
        "last_value",
        "lead",
        "max",
        "min",
        "nth_value",
        "ntile",
        "percent_rank",
        "rank",
        "row_number",
        "sum",
        "stddev",
        "variance",
        "rolling_avg",
        "rolling_max",
        "rolling_min",
        "rolling_sum",
        "select_latest",
        "select_earliest",
    ],
)
def test_supported_v2_window_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="window", name=name))

    assert decision.supported


def test_supported_v2_cache_requirement_passes() -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="optimization", name="cache"))

    assert decision.supported


@pytest.mark.parametrize(
    ("group", "name"),
    [
        ("window", "window_project"),
        ("optimization", "repartition"),
        ("explain", "field_lineage"),
        ("docs", "generated_docs"),
        ("compile", "incremental"),
    ],
)
def test_v2_operation_capabilities_are_explicitly_unsupported(group: str, name: str) -> None:
    resolved = Capabilities.resolve()()

    with pytest.raises(BackendCapabilityError) as raised:
        resolved.require(CapabilityRequirement(group=group, name=name))

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == BACKEND_E2402
    assert diagnostic.feature_group == group
    assert diagnostic.feature_name == name


def test_spark_connect_resolves_as_pyspark_variant() -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": "spark-connect"})

    assert resolved.id.name == "pyspark"
    assert resolved.id.target == ">=3.5,<4.1"
    assert resolved.id.variant == "spark-connect"
    assert resolved.id.family == "spark_connect_dataframe"
    assert resolved.require(CapabilityRequirement(group="backend", name="spark_connect_dataframe")).supported


@pytest.mark.parametrize(
    "name",
    [
        "group_by",
        "count",
        "count_distinct",
        "sum",
        "min",
        "max",
        "mode",
        "avg",
        "grouping_sets",
        "having",
    ],
)
def test_spark_connect_accepts_completed_aggregate_batch_capabilities(name: str) -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": "spark-connect"})

    assert resolved.require(CapabilityRequirement(group="aggregate", name=name)).supported


@pytest.mark.parametrize(
    ("group", "name"),
    [
        ("join", "exists"),
        ("join", "not_exists"),
        ("join", "inner_join"),
        ("join", "lookup_dedupe"),
        ("join", "temporal_one"),
        ("dedupe", "drop_duplicates"),
        ("higher_order", "array_transform"),
        ("higher_order", "array_filter"),
        ("higher_order", "map_transform_values"),
        ("higher_order", "map_filter"),
        ("window", "row_number"),
        ("window", "rank"),
        ("window", "dense_rank"),
        ("window", "lag"),
        ("window", "lead"),
        ("window", "rolling_sum"),
        ("window", "rolling_avg"),
        ("window", "rolling_min"),
        ("window", "rolling_max"),
        ("window", "select_latest"),
        ("window", "select_earliest"),
    ],
)
def test_spark_connect_accepts_completed_compiler_visible_batch_capabilities(group: str, name: str) -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": "spark-connect"})

    assert resolved.require(CapabilityRequirement(group=group, name=name)).supported


@pytest.mark.parametrize("name", ["spark_context", "rdd_access", "jvm_access", "private_classic_fields"])
def test_spark_connect_rejects_classic_only_backend_internals(name: str) -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": "spark-connect"})

    with pytest.raises(BackendCapabilityError) as raised:
        resolved.require(CapabilityRequirement(group="backend", name=name))

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == BACKEND_E2402
    assert diagnostic.backend == "pyspark"
    assert diagnostic.context()["target_variant"] == "spark-connect"
    assert "explicit hook" in diagnostic.use


@pytest.mark.parametrize("name", ["spark_context", "rdd_access", "jvm_access", "private_classic_fields"])
def test_ordinary_pyspark_accepts_classic_backend_internals(name: str) -> None:
    resolved = Capabilities.resolve()(target="pyspark", options={"variant": "ordinary"})

    assert resolved.require(CapabilityRequirement(group="backend", name=name)).supported


def test_spark_connect_is_not_a_separate_backend_id() -> None:
    try:
        Capabilities.resolve()(target="spark_connect", options={"variant": "spark-connect"})
    except BackendCapabilityError as error:
        diagnostic = error.diagnostic
    else:
        raise AssertionError("spark_connect should remain a rejected backend id")

    assert diagnostic.code == BACKEND_E2401
    assert diagnostic.backend == "spark_connect"
    assert diagnostic.feature_group == "backend"
    assert diagnostic.feature_name == "spark_connect"
    assert diagnostic.context()["target_backend"] == "spark_connect"
    assert "plugin.default" in diagnostic.use
    assert "pyspark" in diagnostic.use


def test_unknown_pyspark_variant_uses_capability_diagnostic() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Capabilities.resolve()(target="pyspark", options={"variant": "classic"})

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == BACKEND_E2402
    assert diagnostic.backend == "pyspark"
    assert diagnostic.feature_group == "backend"
    assert diagnostic.feature_name == "unknown"
    assert 'target_variant = "ordinary"' in diagnostic.use


def test_static_fixtures_evaluate_same_requirement_without_runtime_spark() -> None:
    requirement = CapabilityRequirement(group="join", name="lookup_join")
    default = PySparkCapabilities()
    restricted = PySparkCapabilities(supported=frozenset({("expression", "literal")}))

    assert default.supports(requirement).supported
    assert not restricted.supports(requirement).supported
    assert "pyspark" not in {name for name in sys.modules if name == "pyspark"}


def test_try_cast_requires_the_pyspark_4_profile() -> None:
    requirement = CapabilityRequirement(group="expression", name="try_cast")

    assert not PySparkCapabilities().supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.0,<4.1").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.2,<4.3").supports(requirement).supported


def test_variant_schema_requires_the_pyspark_4_profile() -> None:
    requirement = CapabilityRequirement(group="schema", name="variant")

    assert not PySparkCapabilities().supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.0,<4.1").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.2,<4.3").supports(requirement).supported


def test_variant_schema_aggregate_requires_the_pyspark_4_profile() -> None:
    requirement = CapabilityRequirement(group="aggregate", name="schema_of_variant_agg")

    assert not PySparkCapabilities().supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.0,<4.1").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.2,<4.3").supports(requirement).supported


@pytest.mark.parametrize("name", ("variant_explode", "variant_explode_outer"))
def test_variant_tvfs_require_the_pyspark_4_profile(name: str) -> None:
    requirement = CapabilityRequirement(group="generator", name=name)

    assert not PySparkCapabilities().supports(requirement).supported
    assert not PySparkCapabilities(target_profile=">=3.5,<4.0").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.0,<4.1").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.2,<4.3").supports(requirement).supported


def test_valid_variant_requires_the_pyspark_4_2_profile() -> None:
    requirement = CapabilityRequirement(group="expression", name="is_valid_variant")

    assert not PySparkCapabilities().supports(requirement).supported
    assert not PySparkCapabilities(target_profile=">=4.0,<4.1").supports(requirement).supported
    assert PySparkCapabilities(target_profile=">=4.2,<4.3").supports(requirement).supported


@pytest.mark.parametrize(
    "name",
    (
        "variant_array_append",
        "try_variant_array_append",
        "variant_insert",
        "try_variant_insert",
        "variant_set",
        "try_variant_set",
    ),
)
def test_variant_mutation_helpers_are_deferred_until_pyspark_4_3_is_released(name: str) -> None:
    requirement = CapabilityRequirement(group="expression", name=name)

    assert not PySparkCapabilities(target_profile=">=4.2,<4.3").supports(requirement).supported
    assert not PySparkCapabilities(target_profile=">=4.3,<4.4").supports(requirement).supported
    assert not PySparkCapabilities(target_profile=">=5.0,<5.1").supports(requirement).supported


def test_variant_delete_is_deferred_until_a_later_spark_profile_is_released() -> None:
    requirement = CapabilityRequirement(group="expression", name="variant_delete")

    assert not PySparkCapabilities(target_profile=">=4.2,<4.3").supports(requirement).supported
    assert not PySparkCapabilities(target_profile=">=4.3,<4.4").supports(requirement).supported
    assert not PySparkCapabilities(target_profile=">=5.0,<5.1").supports(requirement).supported


def test_generated_import_names_are_deterministic_for_same_target() -> None:
    first = Capabilities.resolve()().imports().as_dict()
    second = Capabilities.resolve()().imports().as_dict()

    assert first == second
    assert list(first) == sorted(first)
    assert first["functions_alias"] == "F"
    assert first["types_alias"] == "T"
