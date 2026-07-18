import pytest

from structure import *
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.types.ArrayType import ArrayType
from structure.core.dsl.model.types.IntegerType import IntegerType
from structure.core.dsl.model.types.LongType import LongType
from structure.core.dsl.model.types.MapType import MapType
from structure.core.dsl.model.types.StringType import StringType
from structure.core.target.pyspark.api import PySpark
from structure.core.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class CollectionSource(Schema):
    id = field.string(nullable=False)
    tags = field.array(field.string(), contains_null=False, nullable=True)
    extra_tags = field.array(field.string(), contains_null=False, nullable=True)
    attributes = field.map(field.string(), field.string(), nullable=True)
    extra_attributes = field.map(field.string(), field.string(), nullable=True)


class CollectionSummary(Schema):
    id = field.string(nullable=False)
    tag_count = field.integer(nullable=True)
    has_priority = field.boolean(nullable=True)
    has_region = field.boolean(nullable=True)
    defaults = field.array(field.string(), contains_null=False, nullable=False)
    repeated = field.array(field.string(), contains_null=False, nullable=False)
    unioned = field.array(field.string(), contains_null=False, nullable=True)
    excluded = field.array(field.string(), contains_null=False, nullable=True)
    intersected = field.array(field.string(), contains_null=False, nullable=True)
    first_two_tags = field.array(field.string(), contains_null=False, nullable=True)
    tag_sequence = field.array(field.integer(), contains_null=False, nullable=False)
    appended_tags = field.array(field.string(), contains_null=False, nullable=True)
    prepended_tags = field.array(field.string(), contains_null=False, nullable=True)
    inserted_tags = field.array(field.string(), contains_null=False, nullable=True)
    removed_tags = field.array(field.string(), contains_null=False, nullable=True)
    compacted_tags = field.array(field.string(), contains_null=False, nullable=True)
    sorted_tags = field.array(field.string(), contains_null=False, nullable=True)
    reversed_tags = field.array(field.string(), contains_null=False, nullable=True)
    first_tag = field.string(nullable=True)
    safe_tag = field.string(nullable=True)
    region = field.string(nullable=True)
    safe_region = field.string(nullable=True)
    merged = field.map(field.string(), field.string(), nullable=True)


@transform
class CollectionHelperTransform(Transform):
    rows = input(CollectionSource)
    summary = output(CollectionSummary)

    def summarize(self, row: CollectionSource) -> CollectionSummary:
        return CollectionSummary(
            id=row.id,
            tag_count=size(row.tags),
            has_priority=array_contains(row.tags, "priority"),
            has_region=map_contains_key(row.attributes, "region"),
            defaults=array("priority", "standard"),
            repeated=array_repeat("priority", 2),
            unioned=array_union(row.tags, row.extra_tags),
            excluded=array_except(row.tags, row.extra_tags),
            intersected=array_intersect(row.tags, row.extra_tags),
            first_two_tags=slice(row.tags, 1, 2),
            tag_sequence=sequence(1, 3),
            appended_tags=arr_append(row.tags, "tail"),
            prepended_tags=arr_prepend(row.tags, "head"),
            inserted_tags=arr_insert(row.tags, 1, "first"),
            removed_tags=arr_remove(row.tags, "deprecated"),
            compacted_tags=arr_compact(row.tags),
            sorted_tags=arr_sort(row.tags),
            reversed_tags=arr_reverse(row.tags),
            first_tag=element_at(row.tags, 1),
            safe_tag=try_element_at(row.tags, 2),
            region=element_at(row.attributes, "region"),
            safe_region=try_element_at(row.attributes, "region"),
            merged=map_concat(row.attributes, row.extra_attributes),
        )


def test_collection_helpers_render_as_readable_pyspark_functions() -> None:
    plan = PySpark.plan.lower()(compile_transform(CollectionHelperTransform))

    text = render_pyspark_step(plan.steps[0], current="rows", sources={"rows": "rows"})

    assert 'F.size(F.col("collection_source.tags")).alias("tag_count")' in text
    assert "F.array_contains(F.col(\"collection_source.tags\"), 'priority').alias(\"has_priority\")" in text
    assert "F.map_contains_key(F.col(\"collection_source.attributes\"), 'region').alias(\"has_region\")" in text
    assert "F.array(F.lit('priority'), F.lit('standard')).alias(\"defaults\")" in text
    assert "F.array_repeat(F.lit('priority'), 2).alias(\"repeated\")" in text
    assert 'F.array_union(F.col("collection_source.tags"), F.col("collection_source.extra_tags"))' in text
    assert 'F.array_except(F.col("collection_source.tags"), F.col("collection_source.extra_tags"))' in text
    assert 'F.array_intersect(F.col("collection_source.tags"), F.col("collection_source.extra_tags"))' in text
    assert 'F.slice(F.col("collection_source.tags"), 1, 2)' in text
    assert 'F.sequence(F.lit(1), F.lit(3))' in text
    assert 'F.array_append(F.col("collection_source.tags"), F.lit(\'tail\'))' in text
    assert 'F.array_prepend(F.col("collection_source.tags"), F.lit(\'head\'))' in text
    assert 'F.array_insert(F.col("collection_source.tags"), 1, F.lit(\'first\'))' in text
    assert 'F.array_remove(F.col("collection_source.tags"), \'deprecated\')' in text
    assert 'F.array_compact(F.col("collection_source.tags"))' in text
    assert 'F.array_sort(F.col("collection_source.tags"))' in text
    assert 'F.reverse(F.col("collection_source.tags"))' in text
    assert 'F.element_at(F.col("collection_source.tags"), F.lit(1)).alias("first_tag")' in text
    assert 'F.try_element_at(F.col("collection_source.tags"), F.lit(2)).alias("safe_tag")' in text
    assert 'F.element_at(F.col("collection_source.attributes"), F.lit(\'region\'))' in text
    assert 'F.map_concat(F.col("collection_source.attributes"), F.col("collection_source.extra_attributes"))' in text


def test_explain_names_collection_helpers_and_their_inputs() -> None:
    report = render_explain_report(CollectionHelperTransform)

    assert "collection helpers: collection_size(tags), array_contains(tags), map_contains_key(attributes)" in report
    assert "element_at(tags), try_element_at(tags)" in report
    assert "map_concat(attributes,extra_attributes)" in report


def test_collection_helpers_reject_ambiguous_or_incompatible_inputs() -> None:
    tags = Expression(kind="field", type=ArrayType(StringType(), contains_null=False), nullable=False)
    scores = Expression(kind="field", type=ArrayType(IntegerType(), contains_null=False), nullable=False)
    attributes = Expression(kind="field", type=MapType(StringType(), StringType()), nullable=False)

    with pytest.raises(TypeError, match="at least one typed value"):
        array()
    with pytest.raises(TypeError, match="compatible types"):
        array("priority", 1)
    with pytest.raises(TypeError, match="compatible types"):
        array_contains(tags, 1)
    with pytest.raises(TypeError, match="map key type"):
        map_contains_key(attributes, 1)
    with pytest.raises(TypeError, match="integral"):
        array_repeat("priority", "two")
    with pytest.raises(TypeError, match="compatible types"):
        array_union(tags, scores)
    with pytest.raises(TypeError, match="compatible types"):
        array_intersect(tags, scores)
    with pytest.raises(TypeError, match="integral"):
        slice(tags, "one", 2)
    with pytest.raises(TypeError, match="must not be negative"):
        slice(tags, 1, -1)
    with pytest.raises(TypeError, match="orderable"):
        arr_sort(Expression(kind="field", type=ArrayType(MapType(StringType(), StringType())), nullable=False))
    with pytest.raises(TypeError, match="integer or long"):
        sequence("start", "stop")
    with pytest.raises(TypeError, match="must not be zero"):
        sequence(1, 3, 0)
    with pytest.raises(TypeError, match="Python literal"):
        arr_insert(tags, scores[0], "first")
    with pytest.raises(TypeError, match="cannot be zero"):
        arr_insert(tags, 0, "first")
    with pytest.raises(TypeError, match="non-null Python literal"):
        arr_remove(tags, scores[0])
    with pytest.raises(TypeError, match="cannot be zero"):
        element_at(tags, 0)
    with pytest.raises(TypeError, match='duplicates="error"'):
        map_concat(attributes, attributes, duplicates="last_win")


def test_array_construction_unifies_integral_literals() -> None:
    expression = array(1, 2**31)

    assert expression.type == ArrayType(LongType(), contains_null=False)
