import importlib
import sys
from typing import cast

from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def test_v4_fixture_skeleton_is_importable_without_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}
    for module in (
        "testing.model.v4",
        "testing.model.v4.orders",
        "testing.model.v4.orders.schemas.scalar",
        "testing.model.v4.orders.schemas",
        "testing.model.v4.orders.transforms.scalar",
        "testing.model.v4.orders.transforms",
    ):
        importlib.import_module(module)
    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before


def test_v4_fixture_starts_with_typed_bitwise_projection() -> None:
    fixture = importlib.import_module("testing.model.v4.orders.transforms.scalar")

    plan = cast(
        TransformPlan,
        Compiler.frontend.compile()(fixture.BitwiseFeatures, materialize_schemas=False).analysis,
    )
    expressions = [
        assignment.expression
        for assignment in cast(PySparkStepBody, plan.steps[0].plugin_body).projection
    ]

    assert [expression.kind for expression in expressions[:6]] == [
        "bitwise_and",
        "bitwise_or",
        "bitwise_xor",
        "bitwise_not",
        "startswith",
        "endswith",
    ]
    assert [expression.kind for expression in expressions[6:]] == ["call"] * 18 + ["transform_expression"] * 3
    assert [expression.data["function"] for expression in expressions[6:] if expression.data is not None] == [
        "nullif",
        "nanvl",
        "ltrim",
        "rtrim",
        "bround",
        "sqrt",
        "pow",
        "log",
        "exp",
        "signum",
        "date_sub",
        "trunc",
        "year",
        "hour",
        "to_date",
        "to_timestamp",
        "hash",
        "sha2",
        "array_slice",
        "array_sort",
        "array_sequence",
    ]
