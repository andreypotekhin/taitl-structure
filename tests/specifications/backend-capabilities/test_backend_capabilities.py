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
    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before


def test_supported_v1_requirement_passes() -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="join", name="join_one"))

    assert decision.supported
    assert decision.code == ""


@pytest.mark.parametrize("name", ["exists", "not_exists", "left_semi_join", "left_anti_join"])
def test_supported_v2_existence_join_requirement_passes(name: str) -> None:
    resolved = Capabilities.resolve()()

    decision = resolved.require(CapabilityRequirement(group="join", name=name))

    assert decision.supported


def test_unsupported_feature_uses_backend_capability_diagnostic() -> None:
    resolved = Capabilities.resolve()()

    try:
        resolved.require(CapabilityRequirement(group="join", name="join_many"))
    except BackendCapabilityError as error:
        diagnostic = error.diagnostic
    else:
        raise AssertionError("join_many should be unsupported in the v1 PySpark profile")

    assert diagnostic.code == BACKEND_E2402
    assert diagnostic.backend == "pyspark"
    assert diagnostic.target == ">=3.5,<4.1"
    assert diagnostic.feature_group == "join"
    assert diagnostic.feature_name == "join_many"
    assert "supported v1 Structure operation" in diagnostic.use
    assert diagnostic.docs == "docs/specifications/BackendCapabilities.md"


@pytest.mark.parametrize(
    ("group", "name"),
    [
        ("join", "join_many"),
        ("join", "temporal_one"),
        ("join", "as_of_one"),
        ("aggregate", "group_by"),
        ("aggregate", "count"),
        ("aggregate", "sum"),
        ("window", "window_project"),
        ("higher_order", "array_transform"),
        ("higher_order", "array_filter"),
        ("optimization", "cache"),
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


def test_unknown_backend_uses_backend_target_diagnostic() -> None:
    try:
        Capabilities.resolve()(target_backend="spark_connect")
    except BackendCapabilityError as error:
        diagnostic = error.diagnostic
    else:
        raise AssertionError("spark_connect should not resolve before a backend profile exists")

    assert diagnostic.code == BACKEND_E2401
    assert diagnostic.backend == "spark_connect"
    assert diagnostic.feature_group == "backend"
    assert diagnostic.feature_name == "spark_connect"
    assert diagnostic.context()["target_backend"] == "spark_connect"
    assert "pyspark" in diagnostic.use


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
