import sys

from structure.app.target.capabilities.api import BackendCapabilityError, Capabilities, CapabilityRequirement


def test_default_pyspark_support_range_resolves_without_importing_pyspark() -> None:
    """I can rely on a documented PySpark support range."""

    before = {name for name in sys.modules if name.startswith("pyspark")}

    resolved = Capabilities.resolve()()

    assert resolved.id.name == "pyspark"
    assert resolved.id.target == ">=3.5,<4.1"
    assert {name for name in sys.modules if name.startswith("pyspark")} == before


def test_v1_backend_profile_accepts_supported_lookup_joins() -> None:
    """The v1 PySpark backend profile accepts supported lookup joins."""

    decision = Capabilities.resolve()().require(CapabilityRequirement(group="join", name="join_one"))

    assert decision.supported
    assert decision.code == ""


def test_v2_join_many_remains_an_explicit_unsupported_capability() -> None:
    """Unsupported v2 capabilities produce backend diagnostics."""

    try:
        Capabilities.resolve()().require(CapabilityRequirement(group="join", name="join_many"))
    except BackendCapabilityError as error:
        diagnostic = error.diagnostic
    else:
        raise AssertionError("join_many should not be supported by the v1 PySpark profile")

    assert diagnostic.code == "BACKEND-E2402"
    assert diagnostic.backend == "pyspark"
    assert diagnostic.target == ">=3.5,<4.1"
    assert diagnostic.feature_group == "join"
    assert diagnostic.feature_name == "join_many"
