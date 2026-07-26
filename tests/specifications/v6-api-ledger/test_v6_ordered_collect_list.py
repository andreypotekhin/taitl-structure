from typing import cast

from structure import Schema, Transform, input, output
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import array, collect_list, integer, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


def test_ordered_collect_list_preserves_the_declared_sort_key_in_generated_source() -> None:
    class Row(Schema):
        group = string(nullable=False)
        ordinal = integer(nullable=False)
        value = string(nullable=False)

    class Summary(Schema):
        values = array(string(), contains_null=False, nullable=False)

    class Collect(Transform):
        rows = input(Row)
        summary = output(Summary)

        def summarize(self, row: Row) -> Summary:
            return Summary(values=collect_list(row.value, order_by=row.ordinal.desc()))

    lowered = cast(PySparkExecutionPlan, Compiler.frontend.compile()(Collect, materialize_schemas=False).lowered)
    aggregate = lowered.steps[0].aggregate
    assert aggregate is not None
    assignment = aggregate.assignments[0]

    assert assignment.function == "collect_list"
    assert assignment.order_by is not None and assignment.order_by.kind == "order"

    rendered = render_pyspark_step(lowered.steps[0], current="rows", sources={"rows": "rows"})

    assert "F.collect_list(F.when(" in rendered
    assert "F.sort_array(" in rendered
    assert "asc=False" in rendered
