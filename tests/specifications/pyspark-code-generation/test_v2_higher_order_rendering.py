from structure import (
    Array,
    String,
    Structure,
    Transform,
    arr_filter,
    arr_transform,
    field,
    input,
    lower,
    output,
    transform,
    trim,
)
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class RawTags(Structure):
    id = field(String(), nullable=False)
    tags = field(Array(String(), contains_null=False), nullable=True)


class CleanTags(Structure):
    id = field(String(), nullable=False)
    tags = field(Array(String(), contains_null=False), nullable=True)


@transform
class CleanTagTransform(Transform):
    rows = input(RawTags)
    clean = output(CleanTags)

    def clean_tags(self, row: RawTags) -> CleanTags:
        tags = arr_filter(arr_transform(row.tags, lambda tag: lower(trim(tag))), lambda tag: tag.is_not_null())
        return CleanTags(id=row.id, tags=tags)


def test_array_higher_order_helpers_render_spark_visible_lambdas() -> None:
    plan = PySpark.plan.lower()(compile_transform(CleanTagTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert (
        'F.filter(F.transform(F.col("raw_tags.tags"), lambda item: F.lower(F.trim(item))), '
        "lambda item: item.isNotNull()).alias(\"tags\")"
    ) in text
