from structure import *
from structure.platform.pyspark import PySpark, field, types
from structure.platform.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class RawTags(Schema):
    id = field.string(nullable=False)
    tags = field.array(field.string(), contains_null=False, nullable=True)


class CleanTags(Schema):
    id = field.string(nullable=False)
    tags = field.array(field.string(), contains_null=False, nullable=True)


class TagSummary(Schema):
    id = field.string(nullable=False)
    has_priority = field.boolean(nullable=True)
    tags = field.array(field.string(), contains_null=False, nullable=True)
    position = field.long(nullable=True)


class TagTextSummary(Schema):
    id = field.string(nullable=False)
    text = field.string(nullable=True)


class SortedTags(Schema):
    id = field.string(nullable=False)
    tags = field.array(field.string(), contains_null=False, nullable=True)


class RawAttributes(Schema):
    id = field.string(nullable=False)
    attributes = field.map(field.string(), field.string(), value_contains_null=True, nullable=True)


class CleanAttributes(Schema):
    id = field.string(nullable=False)
    attributes = field.map(field.string(), field.string(), value_contains_null=False, nullable=True)
    keys = field.array(field.string(), contains_null=False, nullable=True)


@transform
class CleanTagTransform(Transform):
    rows = input(RawTags)
    clean = output(CleanTags)

    def clean_tags(self, row: RawTags) -> CleanTags:
        tags = arr_filter(
            arr_transform(row.tags, lambda tag: lower(trim(tag))),
            lambda tag: tag.is_not_null(),
        )
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
class TagTextSummaryTransform(Transform):
    rows = input(RawTags)
    summary = output(TagTextSummary)

    def summarize_tags(self, row: RawTags) -> TagTextSummary:
        return TagTextSummary(
            id=row.id,
            text=arr_aggregate(
                row.tags,
                "",
                lambda accumulator, item: concat_ws("", accumulator, item),
                finish=lambda accumulator: upper(accumulator),
            ),
        )


@transform
class SortedTagsTransform(Transform):
    rows = input(RawTags)
    sorted_tags = output(SortedTags)

    def sort_tags(self, row: RawTags) -> SortedTags:
        return SortedTags(id=row.id, tags=arr_sort_by(row.tags, lambda tag: lower(trim(tag))))


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
    plan = PySpark.compiler.lower()(compile_transform(CleanTagTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert (
        'F.filter(F.transform(F.col("raw_tags.tags"), lambda item: F.lower(F.trim(item))), '
        "lambda item: item.isNotNull()).alias(\"tags\")"
    ) in text


def test_map_higher_order_helpers_render_spark_visible_lambdas() -> None:
    plan = PySpark.compiler.lower()(compile_transform(CleanAttributeTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert (
        'F.map_filter(F.transform_keys(F.transform_values(F.col("raw_attributes.attributes"), '
        "lambda key, value: F.lower(F.trim(value))), lambda key, value: F.lower(F.trim(key))), "
        "lambda key, value: value.isNotNull()).alias(\"attributes\")"
    ) in text
    assert 'F.map_keys(F.col("raw_attributes.attributes")).alias("keys")' in text


def test_advanced_array_higher_order_helpers_render_spark_visible_lambdas() -> None:
    plan = PySpark.compiler.lower()(compile_transform(TagSummaryTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert 'F.exists(F.col("raw_tags.tags"), lambda item: (F.lower(F.trim(item)) == F.lit' in text
    assert (
        'F.array_distinct(F.zip_with(F.col("raw_tags.tags"), F.col("raw_tags.tags"), '
        "lambda left_item, right_item: F.lower(F.trim(left_item)))).alias(\"tags\")"
    ) in text
    assert 'F.array_position(F.col("raw_tags.tags"), ' in text
    assert "F.array_position(F.col(\"raw_tags.tags\"), 'priority')" in text


def test_array_aggregate_renders_its_finish_callback_against_the_final_accumulator() -> None:
    plan = PySpark.compiler.lower()(compile_transform(TagTextSummaryTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert (
        'F.aggregate(F.col("raw_tags.tags"), F.lit(\'\'), '
        "lambda acc, item: F.concat_ws('', acc, item), lambda acc: F.upper(acc)).alias(\"text\")"
    ) in text


def test_array_sort_by_renders_its_symbolic_key_as_a_spark_comparator() -> None:
    plan = PySpark.compiler.lower()(compile_transform(SortedTagsTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert 'F.array_sort(F.col("raw_tags.tags"), lambda left, right:' in text
    assert "F.lower(F.trim(left))" in text
    assert "F.lower(F.trim(right))" in text
    assert "F.sort_array" not in text
