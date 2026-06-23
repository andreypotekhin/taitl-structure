import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from structure.app.target.capabilities.api.capabilities import CapabilityRequirement, resolve_backend_capabilities
from structure.app.target.capabilities.logic.model.capabilities import BackendCapabilityError
from structure.app.target.capabilities.logic.model.diagnostics import BACKEND_E2401, BACKEND_E2402
from structure.app.target.capabilities.logic.rules.PySparkCapabilityRules import PySparkCapabilities


def test_default_pyspark_capabilities_do_not_import_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    capabilities = resolve_backend_capabilities()

    assert capabilities.id.name == "pyspark"
    assert capabilities.id.target == ">=3.5,<4.1"
    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before


def test_supported_v1_requirement_passes() -> None:
    capabilities = resolve_backend_capabilities()

    decision = capabilities.require(CapabilityRequirement(group="join", name="join_one"))

    assert decision.supported
    assert decision.code == ""


def test_unsupported_feature_uses_backend_capability_diagnostic() -> None:
    capabilities = resolve_backend_capabilities()

    try:
        capabilities.require(CapabilityRequirement(group="join", name="join_many"))
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


def test_unknown_backend_uses_backend_target_diagnostic() -> None:
    try:
        resolve_backend_capabilities(target_backend="spark_connect")
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
    first = resolve_backend_capabilities().imports().as_dict()
    second = resolve_backend_capabilities().imports().as_dict()

    assert first == second
    assert list(first) == sorted(first)
    assert first["functions_alias"] == "F"
    assert first["types_alias"] == "T"
