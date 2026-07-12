import structure
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class RawTags(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


class CleanTags(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


class TagSummary(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    has_priority = structure.field(structure.Boolean(), nullable=True)
    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)
    position = structure.field(structure.Long(), nullable=True)


class RawAttributes(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    attributes = structure.field(
        structure.Map(structure.String(), structure.String(), value_contains_null=True), nullable=True
    )


class CleanAttributes(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    attributes = structure.field(
        structure.Map(structure.String(), structure.String(), value_contains_null=False), nullable=True
    )
    keys = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


@structure.transform
class CleanTagTransform(structure.Transform):
    rows = structure.input(RawTags)
    clean = structure.output(CleanTags)

    def clean_tags(self, row: RawTags) -> CleanTags:
        tags = structure.arr_filter(
            structure.arr_transform(row.tags, lambda tag: structure.lower(structure.trim(tag))),
            lambda tag: tag.is_not_null(),
        )
        return CleanTags(id=row.id, tags=tags)


@structure.transform
class TagSummaryTransform(structure.Transform):
    rows = structure.input(RawTags)
    summary = structure.output(TagSummary)

    def summarize_tags(self, row: RawTags) -> TagSummary:
        tags = structure.arr_distinct(
            structure.arr_zip_with(row.tags, row.tags, lambda left, right: structure.lower(structure.trim(left)))
        )
        return TagSummary(
            id=row.id,
            has_priority=structure.arr_exists(row.tags, lambda tag: structure.lower(structure.trim(tag)) == "priority"),
            tags=tags,
            position=structure.arr_position(row.tags, "priority"),
        )


@structure.transform
class CleanAttributeTransform(structure.Transform):
    rows = structure.input(RawAttributes)
    clean = structure.output(CleanAttributes)

    def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
        attributes = structure.map_filter(
            structure.map_transform_keys(
                structure.map_transform_values(
                    row.attributes, lambda key, value: structure.lower(structure.trim(value))
                ),
                lambda key, value: structure.lower(structure.trim(key)),
            ),
            lambda key, value: value.is_not_null(),
        )
        return CleanAttributes(id=row.id, attributes=attributes, keys=structure.map_keys(row.attributes))


def test_array_higher_order_helpers_render_spark_visible_lambdas() -> None:
    plan = PySpark.plan.lower()(compile_transform(CleanTagTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert (
        'F.filter(F.transform(F.col("raw_tags.tags"), lambda item: F.lower(F.trim(item))), '
        "lambda item: item.isNotNull()).alias(\"tags\")"
    ) in text


def test_map_higher_order_helpers_render_spark_visible_lambdas() -> None:
    plan = PySpark.plan.lower()(compile_transform(CleanAttributeTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert (
        'F.map_filter(F.transform_keys(F.transform_values(F.col("raw_attributes.attributes"), '
        "lambda key, value: F.lower(F.trim(value))), lambda key, value: F.lower(F.trim(key))), "
        "lambda key, value: value.isNotNull()).alias(\"attributes\")"
    ) in text
    assert 'F.map_keys(F.col("raw_attributes.attributes")).alias("keys")' in text


def test_advanced_array_higher_order_helpers_render_spark_visible_lambdas() -> None:
    plan = PySpark.plan.lower()(compile_transform(TagSummaryTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert 'F.exists(F.col("raw_tags.tags"), lambda item: (F.lower(F.trim(item)) == F.lit' in text
    assert (
        'F.array_distinct(F.zip_with(F.col("raw_tags.tags"), F.col("raw_tags.tags"), '
        "lambda left_item, right_item: F.lower(F.trim(left_item)))).alias(\"tags\")"
    ) in text
    assert 'F.array_position(F.col("raw_tags.tags"), ' in text
    assert "F.array_position(F.col(\"raw_tags.tags\"), 'priority')" in text
