import pytest
from integration.pyspark.support.backend_matrix import _plugin
from integration.pyspark.support.search_options import stage_outputs_enabled


def test_stage_outputs_default_to_disabled(monkeypatch) -> None:
    monkeypatch.delenv("STRUCTURE_SEARCH_STAGE_OUTPUTS", raising=False)

    assert stage_outputs_enabled() is False


@pytest.mark.parametrize(("value", "expected"), [("0", False), ("1", True)])
def test_stage_outputs_accept_only_explicit_binary_values(monkeypatch, value, expected) -> None:
    monkeypatch.setenv("STRUCTURE_SEARCH_STAGE_OUTPUTS", value)

    assert stage_outputs_enabled() is expected


@pytest.mark.parametrize("value", ["", "true", "yes", "2"])
def test_stage_outputs_reject_ambiguous_values(monkeypatch, value) -> None:
    monkeypatch.setenv("STRUCTURE_SEARCH_STAGE_OUTPUTS", value)

    with pytest.raises(ValueError, match="must be exactly 0 or 1"):
        stage_outputs_enabled()


def test_removed_boundary_override_points_to_new_name(monkeypatch):
    monkeypatch.setenv("STRUCTURE_CONNECT_PLAN_BOUNDARIES", "off")
    with pytest.raises(ValueError, match="use STRUCTURE_PLAN_BOUNDARIES instead"):
        _plugin()
