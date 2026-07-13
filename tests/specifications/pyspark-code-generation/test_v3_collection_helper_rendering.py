import pytest

import structure
from structure.app.cli.commands.RenderExplainReport import render_explain_report
from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.target.pyspark.api import PySpark
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step


class CollectionSource(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)
    extra_tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)
    attributes = structure.field(structure.Map(structure.String(), structure.String()), nullable=True)
    extra_attributes = structure.field(structure.Map(structure.String(), structure.String()), nullable=True)


class CollectionSummary(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    tag_count = structure.field(structure.Integer(), nullable=True)
    has_priority = structure.field(structure.Boolean(), nullable=True)
    has_region = structure.field(structure.Boolean(), nullable=True)
    defaults = structure.field(structure.Array(structure.String(), contains_null=False), nullable=False)
    repeated = structure.field(structure.Array(structure.String(), contains_null=False), nullable=False)
    unioned = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)
    excluded = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)
    first_tag = structure.field(structure.String(), nullable=True)
    safe_tag = structure.field(structure.String(), nullable=True)
    region = structure.field(structure.String(), nullable=True)
    safe_region = structure.field(structure.String(), nullable=True)
    merged = structure.field(structure.Map(structure.String(), structure.String()), nullable=True)


@structure.transform
class CollectionHelperTransform(structure.Transform):
    rows = structure.input(CollectionSource)
    summary = structure.output(CollectionSummary)

    def summarize(self, row: CollectionSource) -> CollectionSummary:
        return CollectionSummary(
            id=row.id,
            tag_count=structure.size(row.tags),
            has_priority=structure.array_contains(row.tags, "priority"),
            has_region=structure.map_contains_key(row.attributes, "region"),
            defaults=structure.array("priority", "standard"),
            repeated=structure.array_repeat("priority", 2),
            unioned=structure.array_union(row.tags, row.extra_tags),
            excluded=structure.array_except(row.tags, row.extra_tags),
            first_tag=structure.element_at(row.tags, 1),
            safe_tag=structure.try_element_at(row.tags, 2),
            region=structure.element_at(row.attributes, "region"),
            safe_region=structure.try_element_at(row.attributes, "region"),
            merged=structure.map_concat(row.attributes, row.extra_attributes),
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
        structure.array()
    with pytest.raises(TypeError, match="compatible types"):
        structure.array("priority", 1)
    with pytest.raises(TypeError, match="compatible types"):
        structure.array_contains(tags, 1)
    with pytest.raises(TypeError, match="map key type"):
        structure.map_contains_key(attributes, 1)
    with pytest.raises(TypeError, match="integral"):
        structure.array_repeat("priority", "two")
    with pytest.raises(TypeError, match="compatible types"):
        structure.array_union(tags, scores)
    with pytest.raises(TypeError, match="cannot be zero"):
        structure.element_at(tags, 0)
    with pytest.raises(TypeError, match='duplicates="error"'):
        structure.map_concat(attributes, attributes, duplicates="last_win")


def test_array_construction_unifies_integral_literals() -> None:
    expression = structure.array(1, 2**31)

    assert expression.type == ArrayType(LongType(), contains_null=False)
