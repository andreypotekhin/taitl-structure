from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import array, hierarchy_closure, hierarchy_fallbacks, integer, long, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module


class CohortBand(Schema):
    band_id = string(nullable=False)
    parent_id = string(nullable=True)


class BandClosure(Schema):
    node_id = string(nullable=False)
    ancestor_id = string(nullable=False)
    depth = long(nullable=False)


class UserBand(Schema):
    user_band_id = string(nullable=False)
    band_ids = array(string(), contains_null=False, nullable=False)


class BandFallback(Schema):
    user_band_id = string(nullable=False)
    ordinal = long(nullable=False)
    user_band_fallback_id = string(nullable=True)


class BuildBandClosure(Transform):
    bands = input(CohortBand)
    closures = output(BandClosure)

    def close(self, band: CohortBand) -> BandClosure:
        closure = hierarchy_closure(
            band.band_id,
            parent=band.parent_id,
            as_=BandClosure,
            max_depth=4,
            scope="closure",
        )
        return BandClosure.project(closure)


class BuildBandFallbacks(Transform):
    user_bands = input(UserBand)
    parents = input(CohortBand)
    fallbacks = output(BandFallback)

    def build(self, user_band: UserBand, parent: CohortBand) -> BandFallback:
        fallbacks = hierarchy_fallbacks(
            user_band.user_band_id,
            user_band.band_ids,
            parent,
            parent_id=parent.band_id,
            parent=parent.parent_id,
            as_=BandFallback,
            max_depth=5,
            scope="fallbacks",
        )
        return BandFallback.project(fallbacks)


def test_hierarchy_closure_is_public_api() -> None:
    assert callable(hierarchy_closure)


def test_hierarchy_fallbacks_is_public_api() -> None:
    assert callable(hierarchy_fallbacks)


def test_hierarchy_closure_records_compiler_visible_operation() -> None:
    operation = _lowered().steps[0].operations[0]

    assert operation.kind == "hierarchy_closure"
    assert operation.relation_hierarchy_closure is not None
    assert operation.relation_hierarchy_closure.schema is BandClosure
    assert operation.relation_hierarchy_closure.scope == "closure"
    assert operation.relation_hierarchy_closure.node == "node_id"
    assert operation.relation_hierarchy_closure.ancestor == "ancestor_id"
    assert operation.relation_hierarchy_closure.depth == "depth"
    assert operation.relation_hierarchy_closure.max_depth == 4


def test_hierarchy_fallbacks_records_compiler_visible_operation() -> None:
    operation = _fallback_lowered().steps[0].operations[0]

    assert operation.kind == "hierarchy_fallbacks"
    assert operation.relation_hierarchy_fallback is not None
    assert operation.relation_hierarchy_fallback.schema is BandFallback
    assert operation.relation_hierarchy_fallback.parent_input == "parent"
    assert operation.relation_hierarchy_fallback.scope == "fallbacks"
    assert operation.relation_hierarchy_fallback.source == "user_band_id"
    assert operation.relation_hierarchy_fallback.fallback == "user_band_fallback_id"
    assert operation.relation_hierarchy_fallback.ordinal == "ordinal"
    assert operation.relation_hierarchy_fallback.max_depth == 5


def test_hierarchy_closure_renders_bounded_self_join_expansion() -> None:
    text = render_pyspark_step(_lowered().steps[0], current="bands", sources={"bands": "bands"})

    assert "bands_hierarchy_closure_0_nodes = bands.select(" in text
    assert 'F.col("cohort_band.band_id").alias("__structure_hierarchy_node_0")' in text
    assert 'F.col("cohort_band.parent_id").alias("__structure_hierarchy_parent_0")' in text
    assert 'F.lit(0).cast(T.LongType()).alias("depth")' in text
    assert 'F.lit(4).cast(T.LongType()).alias("depth")' in text
    assert "unionByName" in text
    assert '.alias("frontier").join(' in text
    assert 'F.col("frontier.__structure_hierarchy_parent_0")' in text


def test_hierarchy_fallbacks_renders_bounded_path_expansion() -> None:
    text = render_pyspark_step(
        _fallback_lowered().steps[0],
        current="user_bands",
        sources={"user_bands": "user_bands", "parents": "parents"},
    )

    assert "user_bands_hierarchy_fallbacks_0_parents = parents.select(" in text
    assert 'F.col("band_id").alias("__structure_fallback_parent_node_0")' in text
    assert 'F.col("user_band.band_ids").alias("__structure_fallback_path_0")' in text
    assert "F.sha2(F.concat_ws('\\x1f', F.col(\"__structure_fallback_path_0\")), 256)" in text
    assert "F.element_at(F.col(\"__structure_fallback_path_0\"), F.lit(-1))" in text
    assert "F.slice(F.col(\"__structure_fallback_path_0\"), F.lit(1)" in text
    assert "F.concat(" in text
    assert "F.lit(5).cast(T.LongType()).alias(\"ordinal\")" in text
    assert "unionByName" in text


def test_hierarchy_closure_generated_module_keeps_operation_helpers_imported() -> None:
    text = render_pyspark_transform_module(
        _lowered(),
        source_transform="tests.BuildBandClosure",
        schema_modules={CohortBand: "tests.schemas", BandClosure: "tests.schemas"},
        runtime_module="tests.runtime",
    )

    assert "class BuildBandClosureGenerated:" in text
    assert "T.LongType()" in text
    assert "unionByName" in text


def test_hierarchy_fallbacks_generated_module_keeps_operation_helpers_imported() -> None:
    text = render_pyspark_transform_module(
        _fallback_lowered(),
        source_transform="tests.BuildBandFallbacks",
        schema_modules={
            UserBand: "tests.schemas",
            CohortBand: "tests.schemas",
            BandFallback: "tests.schemas",
        },
        runtime_module="tests.runtime",
    )

    assert "class BuildBandFallbacksGenerated:" in text
    assert "F.sha2" in text
    assert "T.LongType()" in text
    assert "unionByName" in text


def test_hierarchy_closure_explain_names_cardinality_and_streaming_status() -> None:
    text = render_explain_report(BuildBandClosure)

    assert "operations: hierarchy_closure(row_multiplying scope=closure schema=BandClosure max_depth=4)" in text
    assert "status: compatible" in text


def test_hierarchy_fallbacks_explain_names_cardinality_and_streaming_status() -> None:
    text = render_explain_report(BuildBandFallbacks)

    assert "operations: hierarchy_fallbacks(row_multiplying scope=fallbacks schema=BandFallback parents=parent max_depth=5)" in text
    assert "status: compatible" in text


def test_hierarchy_closure_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.BuildBandClosure",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    closure = dependencies["close.hierarchy_closure[0].closure"]
    assert closure.sources == ("bands.band_id", "bands.parent_id")
    assert closure.operation == "hierarchy_closure"
    assert closure.detail["scope"] == "closure"
    assert closure.detail["schema"] == "BandClosure"
    assert closure.detail["max_depth"] == 4


def test_hierarchy_fallbacks_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _fallback_lowered(),
        source_transform="tests.BuildBandFallbacks",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    fallbacks = dependencies["build.hierarchy_fallbacks[0].fallbacks"]
    assert fallbacks.sources == (
        "user_band.user_band_id",
        "user_band.band_ids",
        "parent.band_id",
        "parent.parent_id",
    )
    assert fallbacks.operation == "hierarchy_fallbacks"
    assert fallbacks.detail["scope"] == "fallbacks"
    assert fallbacks.detail["schema"] == "BandFallback"
    assert fallbacks.detail["parents"] == "parent"
    assert fallbacks.detail["max_depth"] == 5


def test_hierarchy_closure_rejects_invalid_arguments() -> None:
    class BadParent(Schema):
        band_id = string(nullable=False)
        parent_id = integer(nullable=True)

    class BadDepth(Schema):
        node_id = string(nullable=False)
        ancestor_id = string(nullable=False)
        depth = integer(nullable=False)

    class MissingDepth(Transform):
        bands = input(CohortBand)
        closures = output(BandClosure)

        def close(self, band: CohortBand) -> BandClosure:
            return BandClosure.project(
                hierarchy_closure(
                    band.band_id,
                    parent=band.parent_id,
                    as_=BandClosure,
                    max_depth=0,
                )
            )

    class MismatchedParent(Transform):
        bands = input(BadParent)
        closures = output(BandClosure)

        def close(self, band: BadParent) -> BandClosure:
            return BandClosure.project(
                hierarchy_closure(
                    band.band_id,
                    parent=band.parent_id,
                    as_=BandClosure,
                    max_depth=4,
                )
            )

    class WrongDepthType(Transform):
        bands = input(CohortBand)
        closures = output(BadDepth)

        def close(self, band: CohortBand) -> BadDepth:
            return BadDepth.project(
                hierarchy_closure(
                    band.band_id,
                    parent=band.parent_id,
                    as_=BadDepth,
                    max_depth=4,
                )
            )

    with pytest.raises(TypeError, match="positive integer literal"):
        Compiler.frontend.compile()(MissingDepth, materialize_schemas=False)
    with pytest.raises(TypeError, match="same type as id"):
        Compiler.frontend.compile()(MismatchedParent, materialize_schemas=False)
    with pytest.raises(TypeError, match="field must be long"):
        Compiler.frontend.compile()(WrongDepthType, materialize_schemas=False)


def test_hierarchy_fallbacks_rejects_invalid_arguments() -> None:
    class NullablePath(Schema):
        user_band_id = string(nullable=False)
        band_ids = array(string(), contains_null=True, nullable=False)

    class BadFallback(Schema):
        user_band_id = string(nullable=False)
        ordinal = long(nullable=False)
        user_band_fallback_id = string(nullable=False)

    class MissingDepth(Transform):
        user_bands = input(UserBand)
        parents = input(CohortBand)
        fallbacks = output(BandFallback)

        def build(self, user_band: UserBand, parent: CohortBand) -> BandFallback:
            return BandFallback.project(
                hierarchy_fallbacks(
                    user_band.user_band_id,
                    user_band.band_ids,
                    parent,
                    parent_id=parent.band_id,
                    parent=parent.parent_id,
                    as_=BandFallback,
                    max_depth=0,
                )
            )

    class BadPath(Transform):
        user_bands = input(NullablePath)
        parents = input(CohortBand)
        fallbacks = output(BandFallback)

        def build(self, user_band: NullablePath, parent: CohortBand) -> BandFallback:
            return BandFallback.project(
                hierarchy_fallbacks(
                    user_band.user_band_id,
                    user_band.band_ids,
                    parent,
                    parent_id=parent.band_id,
                    parent=parent.parent_id,
                    as_=BandFallback,
                    max_depth=5,
                )
            )

    class BadOutput(Transform):
        user_bands = input(UserBand)
        parents = input(CohortBand)
        fallbacks = output(BadFallback)

        def build(self, user_band: UserBand, parent: CohortBand) -> BadFallback:
            return BadFallback.project(
                hierarchy_fallbacks(
                    user_band.user_band_id,
                    user_band.band_ids,
                    parent,
                    parent_id=parent.band_id,
                    parent=parent.parent_id,
                    as_=BadFallback,
                    max_depth=5,
                )
            )

    with pytest.raises(TypeError, match="positive integer literal"):
        Compiler.frontend.compile()(MissingDepth, materialize_schemas=False)
    with pytest.raises(TypeError, match="contains_null=False"):
        Compiler.frontend.compile()(BadPath, materialize_schemas=False)
    with pytest.raises(TypeError, match="nullable string"):
        Compiler.frontend.compile()(BadOutput, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(BuildBandClosure, materialize_schemas=False).lowered,
    )


def _fallback_lowered() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(BuildBandFallbacks, materialize_schemas=False).lowered,
    )
