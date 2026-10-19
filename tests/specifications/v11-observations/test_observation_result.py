from typing import Any

from structure import Schema
from structure.core.runtime.session.model.ObservationResult import ObservationResult
from structure.core.runtime.session.model.TransformResult import TransformResult
from structure.plugin.pyspark import array, long, map, string, struct


class Metrics(Schema):
    rows = long(nullable=False)


class Detail(Schema):
    label = string(nullable=False)


class NestedMetrics(Schema):
    detail = struct(Detail, nullable=False)
    labels = array(string(), contains_null=False, nullable=False)
    counters = map(string(), long(), value_contains_null=False, nullable=False)


def test_get_constructs_a_fresh_schema_snapshot():
    values = {"rows": 4}
    result = ObservationResult(Metrics, lambda: values)

    first = result.get()
    values["rows"] = 7
    second = result.get()

    assert first.rows == 4
    assert second.rows == 7
    assert first is not second
    assert result.schema is Metrics


def test_get_reports_metrics_that_are_not_available():
    result = ObservationResult(Metrics, dict)

    try:
        result.get()
    except RuntimeError as error:
        assert "run an action" in str(error)
    else:
        raise AssertionError("Unavailable observation metrics must be diagnosed")


def test_get_recursively_rebuilds_and_detaches_nested_values():
    values: dict[str, Any] = {
        "detail": {"label": "before"},
        "labels": ["one"],
        "counters": {"one": 1},
    }
    result = ObservationResult(NestedMetrics, lambda: values)

    first = result.get()
    first.labels.append("mutated")
    first.counters["two"] = 2
    values["detail"]["label"] = "after"
    second = result.get()

    assert first.detail.label == "before"
    assert second.detail.label == "after"
    assert second.labels == ["one"]
    assert second.counters == {"one": 1}


def test_transform_observation_access_does_not_change_output_mapping():
    result = TransformResult(
        {"output": object()},
        observations={"quality": ObservationResult(Metrics, lambda: {"rows": 4})},
        unused_observations=("disabled",),
    )

    assert result.quality.get().rows == 4
    assert list(result) == ["output"]
    assert list(result.as_dict()) == ["output"]
    try:
        result.disabled
    except AttributeError as error:
        assert "unused" in str(error)
    else:
        raise AssertionError("An unused declaration must explain its missing result handle")


def test_stage_observation_access_follows_stage_output_visibility():
    result = TransformResult(
        {"output": object()},
        stage_observations=[(("clean",), "quality", ObservationResult(Metrics, lambda: {"rows": 3}))],
        unused_stage_observations=[(("publish",), "disabled")],
        stage_outputs_enabled=True,
        stage_names=("clean", "publish"),
    )

    assert result.stages["clean"].quality.get().rows == 3
    try:
        result.stages["publish"].disabled
    except AttributeError as error:
        assert "unused in this stage" in str(error)
    else:
        raise AssertionError("An unused stage observation must explain its missing handle")

    hidden = TransformResult(
        {"output": object()}, stage_outputs_enabled=False, stage_names=("clean",)
    )
    try:
        hidden.clean
    except AttributeError as error:
        assert "allow_stage_outputs=True" in str(error)
    else:
        raise AssertionError("Stage access must honor allow_stage_outputs")
