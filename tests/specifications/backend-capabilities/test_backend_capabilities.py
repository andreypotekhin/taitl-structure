import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from structure.app.target.capabilities.api import (
    BACKEND_E2401,
    BACKEND_E2402,
    BackendCapabilityError,
    Capabilities,
    CapabilityRequirement,
    PySparkCapabilities,
)


def test_default_pyspark_capabilities_do_not_import_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    resolved = Capabilities.resolve()()

    assert resolved.id.name == "pyspark"
    assert resolved.id.target == ">=3.5,<4.1"
    assert resolved.id.variant == "ordinary"
    assert resolved.id.family == "ordinary_pyspark"
    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before


def test_supported_v1_requirement_passes() -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="join", name="join_one"))

    assert decision.supported
    assert decision.code == ""


@pytest.mark.parametrize(
    "name", ["exists", "not_exists", "join_many", "temporal_one", "as_of_one", "left_semi_join", "left_anti_join"]
)
def test_supported_v2_join_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="join", name=name))

    assert decision.supported


@pytest.mark.parametrize("name", ["group_by", "count", "count_distinct", "sum", "min", "max", "avg"])
def test_supported_v2_aggregate_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="aggregate", name=name))

    assert decision.supported


@pytest.mark.parametrize("name", ["array_transform", "array_filter", "map_transform_values", "map_filter"])
def test_supported_v2_higher_order_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="higher_order", name=name))

    assert decision.supported


def test_supported_v2_dedupe_requirement_passes() -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="dedupe", name="drop_duplicates"))

    assert decision.supported


@pytest.mark.parametrize(
    "name",
    [
        "dense_rank",
        "lag",
        "lead",
        "rank",
        "row_number",
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
    resolved = Capabilities.resolve()(target_backend="pyspark", target_variant="spark-connect")

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
        "avg",
    ],
)
def test_spark_connect_accepts_completed_aggregate_batch_capabilities(name: str) -> None:
    resolved = Capabilities.resolve()(target_backend="pyspark", target_variant="spark-connect")

    assert resolved.require(CapabilityRequirement(group="aggregate", name=name)).supported


@pytest.mark.parametrize(
    ("group", "name"),
    [
        ("join", "exists"),
        ("join", "not_exists"),
        ("join", "join_many"),
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
    resolved = Capabilities.resolve()(target_backend="pyspark", target_variant="spark-connect")

    assert resolved.require(CapabilityRequirement(group=group, name=name)).supported


@pytest.mark.parametrize("name", ["spark_context", "rdd_access", "jvm_access", "private_classic_fields"])
def test_spark_connect_rejects_classic_only_backend_internals(name: str) -> None:
    resolved = Capabilities.resolve()(target_backend="pyspark", target_variant="spark-connect")

    with pytest.raises(BackendCapabilityError) as raised:
        resolved.require(CapabilityRequirement(group="backend", name=name))

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == BACKEND_E2402
    assert diagnostic.backend == "pyspark"
    assert diagnostic.context()["target_variant"] == "spark-connect"
    assert "explicit hook" in diagnostic.use


@pytest.mark.parametrize("name", ["spark_context", "rdd_access", "jvm_access", "private_classic_fields"])
def test_ordinary_pyspark_accepts_classic_backend_internals(name: str) -> None:
    resolved = Capabilities.resolve()(target_backend="pyspark", target_variant="ordinary")

    assert resolved.require(CapabilityRequirement(group="backend", name=name)).supported


def test_spark_connect_is_not_a_separate_backend_id() -> None:
    try:
        Capabilities.resolve()(target_backend="spark_connect", target_variant="spark-connect")
    except BackendCapabilityError as error:
        diagnostic = error.diagnostic
    else:
        raise AssertionError("spark_connect should remain a rejected backend id")

    assert diagnostic.code == BACKEND_E2401
    assert diagnostic.backend == "spark_connect"
    assert diagnostic.feature_group == "backend"
    assert diagnostic.feature_name == "spark_connect"
    assert diagnostic.context()["target_backend"] == "spark_connect"
    assert "pyspark" in diagnostic.use


def test_unknown_pyspark_variant_uses_capability_diagnostic() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Capabilities.resolve()(target_backend="pyspark", target_variant="classic")

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == BACKEND_E2402
    assert diagnostic.backend == "pyspark"
    assert diagnostic.feature_group == "backend"
    assert diagnostic.feature_name == "unknown"
    assert 'target_variant = "ordinary"' in diagnostic.use


def test_static_fixtures_evaluate_same_requirement_without_runtime_spark() -> None:
    requirement = CapabilityRequirement(group="join", name="join_one")
    default = PySparkCapabilities()
    restricted = PySparkCapabilities(supported=frozenset({("expression", "literal")}))

    assert default.supports(requirement).supported
    assert not restricted.supports(requirement).supported
    assert "pyspark" not in {name for name in sys.modules if name == "pyspark"}


def test_generated_import_names_are_deterministic_for_same_target() -> None:
    first = Capabilities.resolve()().imports().as_dict()
    second = Capabilities.resolve()().imports().as_dict()

    assert first == second
    assert list(first) == sorted(first)
    assert first["functions_alias"] == "F"
    assert first["types_alias"] == "T"
