from structure import (
    Array,
    Boolean,
    Long,
    Map,
    String,
    Structure,
    Transform,
    arr_distinct,
    arr_exists,
    arr_filter,
    arr_position,
    arr_transform,
    arr_zip_with,
    field,
    input,
    lower,
    map_filter,
    map_keys,
    map_transform_keys,
    map_transform_values,
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


class TagSummary(Structure):
    id = field(String(), nullable=False)
    has_priority = field(Boolean(), nullable=True)
    tags = field(Array(String(), contains_null=False), nullable=True)
    position = field(Long(), nullable=True)


class RawAttributes(Structure):
    id = field(String(), nullable=False)
    attributes = field(Map(String(), String(), value_contains_null=True), nullable=True)


class CleanAttributes(Structure):
    id = field(String(), nullable=False)
    attributes = field(Map(String(), String(), value_contains_null=False), nullable=True)
    keys = field(Array(String(), contains_null=False), nullable=True)


@transform
class CleanTagTransform(Transform):
    rows = input(RawTags)
    clean = output(CleanTags)

    def clean_tags(self, row: RawTags) -> CleanTags:
        tags = arr_filter(arr_transform(row.tags, lambda tag: lower(trim(tag))), lambda tag: tag.is_not_null())
        return CleanTags(id=row.id, tags=tags)


@transform
class TagSummaryTransform(Transform):
    rows = input(RawTags)
    summary = output(TagSummary)

    def summarize_tags(self, row: RawTags) -> TagSummary:
        tags = arr_distinct(arr_zip_with(row.tags, row.tags, lambda left, right: lower(trim(left))))
        return TagSummary(
            id=row.id,
            has_priority=arr_exists(row.tags, lambda tag: lower(trim(tag)) == "priority"),
            tags=tags,
            position=arr_position(row.tags, "priority"),
        )


@transform
class CleanAttributeTransform(Transform):
    rows = input(RawAttributes)
    clean = output(CleanAttributes)

    def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
        attributes = map_filter(
            map_transform_keys(
                map_transform_values(row.attributes, lambda key, value: lower(trim(value))),
                lambda key, value: lower(trim(key)),
            ),
            lambda key, value: value.is_not_null(),
        )
        return CleanAttributes(id=row.id, attributes=attributes, keys=map_keys(row.attributes))


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
