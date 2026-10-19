from typing import Any, cast

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl import types
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class Values(Schema):
    left = long(nullable=False)
    right = long(nullable=False)


class Sums(Schema):
    total = long(nullable=False)


def test_transform_can_call_a_named_arrow_expression_helper() -> None:
    """I can reuse a Spark Arrow callback as a typed Transform expression."""

    class AddValues(Transform):
        source = input(Values)
        target = output(Sums)

        @special(type="arrow_udf", return_type=types.long(), deterministic=False)
        def add(left, right):
            import pyarrow.compute as pc  # type: ignore[import-not-found]

            return pc.add(left, right)

        def apply(self, row: Values) -> Sums:
            return Sums(total=self.add(right=row.right, left=row.left))

    plan = Compiler.frontend.compile()(
        AddValues,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.1,<4.2", "variant": "ordinary"}},
    )
    expression = cast(PySparkStepBody, cast(Any, plan).analysis.steps[0].plugin_body).projection[0].expression

    assert expression.kind == "arrow_udf"
    assert expression.data["deterministic"] is False
    assert expression.data["keyword_names"] == ("right", "left")
