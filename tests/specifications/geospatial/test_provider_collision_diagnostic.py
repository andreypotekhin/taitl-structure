from dataclasses import dataclass

import pytest

from structure.plugin.api.v1.model import StepPlan, TransformPlan
from structure.plugin.pyspark import sedona, st_geomfromwkb
from structure.plugin.pyspark.compiler.commands.BuildPySparkExpressionDiagnostics import (
    BuildPySparkExpressionDiagnostics,
)
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class ExpressionBody:
    expressions: tuple[Expression, ...]


def _plan(*expressions: Expression) -> TransformPlan:
    steps = tuple(
        StepPlan(
            name=f"GeoStep{index}",
            input_schema=None,
            output_schema=None,
            source="",
            source_scope="",
            input_lane="",
            output_lane="",
            ordinal=index,
            plugin_body=ExpressionBody(expressions=(expression,)),
        )
        for index, expression in enumerate(expressions)
    )
    return TransformPlan(name="MixedGeoTransform", inputs=(), steps=steps, outputs=())


@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
def test_warns_when_spark_41_transform_uses_apache_and_sedona(variant: str) -> None:
    diagnostics = BuildPySparkExpressionDiagnostics()(
        _plan(st_geomfromwkb(b"wkb"), sedona.st_geomfromwkt("POINT (1 2)", 4326)),
        profile=">=4.1,<4.2",
        variant=variant,
    )

    assert [diagnostic.code for diagnostic in diagnostics] == ["PYSPARK-W2707"]
    assert diagnostics[0].source == "MixedGeoTransform"


@pytest.mark.parametrize(
    ("profile", "variant"),
    [(">=3.5,<4.1", "ordinary"), (">=4.0,<4.1", "ordinary"), (">=4.1,<4.2", "other")],
)
def test_does_not_warn_outside_conflicting_profiles(profile: str, variant: str) -> None:
    diagnostics = BuildPySparkExpressionDiagnostics()(
        _plan(st_geomfromwkb(b"wkb"), sedona.st_geomfromwkt("POINT (1 2)", 4326)),
        profile=profile,
        variant=variant,
    )

    assert diagnostics == ()


def test_does_not_warn_when_only_one_provider_is_used() -> None:
    diagnostics = BuildPySparkExpressionDiagnostics()(
        _plan(st_geomfromwkb(b"one"), st_geomfromwkb(b"two")),
        profile=">=4.1,<4.2",
        variant="ordinary",
    )

    assert diagnostics == ()
