from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    integer,
    require_all,
    require_parent_hierarchy,
    require_reference,
    require_unique,
    string,
)
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class CohortBand(Schema):
    band_id = string(nullable=False)
    parent_id = string(nullable=True)
    priority = integer(nullable=False)


class ValidateBands(Transform):
    bands = input(CohortBand)
    valid = output(CohortBand)

    def validate(self, band: CohortBand) -> CohortBand:
        require_unique(band.band_id)
        asserted = require_all(band.priority >= 0)
        return CohortBand.project(asserted)


class ValidateBandParents(Transform):
    bands = input(CohortBand)
    catalog = input(CohortBand)
    valid = output(CohortBand)

    def validate(self, band: CohortBand, catalog: CohortBand) -> CohortBand:
        asserted = require_reference(band.parent_id, catalog, reference_key=catalog.band_id)
        return CohortBand.project(asserted)


class ValidateBandHierarchy(Transform):
    bands = input(CohortBand)
    valid = output(CohortBand)

    def validate(self, band: CohortBand) -> CohortBand:
        asserted = require_parent_hierarchy(
            band.band_id,
            parent=band.parent_id,
            order_by=band.priority,
            max_depth=4,
        )
        return CohortBand.project(asserted)


def test_relation_assertions_record_compiler_visible_operations() -> None:
    operations = _lowered().steps[0].operations

    assert [operation.kind for operation in operations] == ["require_unique", "require_all"]
    assert operations[0].relation_assertion is not None
    assert len(operations[0].relation_assertion.keys) == 1
    assert operations[1].relation_assertion is not None
    assert operations[1].relation_assertion.predicate is not None


def test_relation_reference_assertion_records_compiler_visible_operation() -> None:
    operation = _lowered_reference().steps[0].operations[0]

    assert operation.kind == "require_reference"
    assert operation.relation_assertion is not None
    assert operation.relation_assertion.reference_input == "catalog"
    assert operation.relation_assertion.reference_schema is CohortBand
    assert operation.relation_assertion.nulls == "allow"


def test_parent_hierarchy_assertion_records_compiler_visible_operation() -> None:
    operation = _lowered_hierarchy().steps[0].operations[0]

    assert operation.kind == "require_parent_hierarchy"
    assert operation.relation_assertion is not None
    assert len(operation.relation_assertion.keys) == 1
    assert operation.relation_assertion.parent is not None
    assert operation.relation_assertion.order_by is not None
    assert operation.relation_assertion.max_depth == 4


def test_relation_assertions_render_spark_visible_assertions() -> None:
    text = render_pyspark_step(_lowered().steps[0], current="bands", sources={"bands": "bands"})

    assert 'bands_require_unique_0_duplicates = bands.groupBy(F.col("cohort_band.band_id"))' in text
    assert "REL-E0702: require_unique" in text
    assert "bands_require_all_1_violations = bands.where(" in text
    assert 'F.coalesce((F.col("cohort_band.priority") >= F.lit(0)), F.lit(False))' in text
    assert "REL-E0703: require_all" in text


def test_relation_reference_assertion_renders_spark_visible_anti_join_assertion() -> None:
    text = render_pyspark_step(
        _lowered_reference().steps[0],
        current="bands",
        sources={"bands": "bands", "catalog": "catalog"},
    )

    assert 'bands_require_reference_0_left = bands.withColumn("__structure_reference_value_0"' in text
    assert 'F.col("cohort_band.parent_id")' in text
    assert 'bands_require_reference_0_right = catalog.select(' in text
    assert 'F.col("band_id").alias("__structure_reference_key_0")' in text
    assert '"left_anti"' in text
    assert "REL-E0704: require_reference" in text


def test_parent_hierarchy_assertion_renders_spark_visible_bounded_checks() -> None:
    text = render_pyspark_step(
        _lowered_hierarchy().steps[0],
        current="bands",
        sources={"bands": "bands"},
    )

    assert "bands_require_parent_hierarchy_0_nodes = bands.select(" in text
    assert 'F.col("cohort_band.band_id").alias("__structure_hierarchy_node_0")' in text
    assert '"left_anti"' in text
    assert "array_contains" in text
    assert "array_append" in text
    assert "REL-E0706: require_parent_hierarchy" in text


def test_relation_assertion_explain_names_cardinality_and_streaming_status() -> None:
    text = render_explain_report(ValidateBands)

    assert "operations: require_unique(row_preserving keys=1), require_all(row_preserving predicate=true)" in text
    assert "STREAM-E0801: batch_only in validate (require_unique)" in text
    assert "STREAM-E0801: batch_only in validate (require_all)" in text


def test_relation_reference_explain_names_reference_and_streaming_status() -> None:
    text = render_explain_report(ValidateBandParents)

    assert "operations: require_reference(row_preserving reference=catalog nulls=allow)" in text
    assert "STREAM-E0801: batch_only in validate (require_reference)" in text


def test_parent_hierarchy_explain_names_depth_and_streaming_status() -> None:
    text = render_explain_report(ValidateBandHierarchy)

    assert "operations: require_parent_hierarchy(row_preserving max_depth=4)" in text
    assert "STREAM-E0801: batch_only in validate (require_parent_hierarchy)" in text


def test_relation_assertions_record_traceability_dependencies() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.ValidateBands",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    unique = dependencies["validate.require_unique[0]"]
    assert unique.sources == ("bands.band_id",)
    assert unique.operation == "require_unique"
    assert unique.detail["diagnostic"] == "REL-E0702"

    predicate = dependencies["validate.require_all[1]"]
    assert predicate.sources == ("bands.priority",)
    assert predicate.operation == "require_all"
    assert predicate.detail["diagnostic"] == "REL-E0703"


def test_relation_reference_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered_reference(),
        source_transform="tests.ValidateBandParents",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    reference = dependencies["validate.require_reference[0]"]
    assert reference.sources == ("band.parent_id", "catalog.band_id")
    assert reference.operation == "require_reference"
    assert reference.detail["diagnostic"] == "REL-E0704"
    assert reference.detail["reference"] == "catalog"


def test_parent_hierarchy_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered_hierarchy(),
        source_transform="tests.ValidateBandHierarchy",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    hierarchy = dependencies["validate.require_parent_hierarchy[0]"]
    assert hierarchy.sources == ("bands.band_id", "bands.parent_id", "bands.priority")
    assert hierarchy.operation == "require_parent_hierarchy"
    assert hierarchy.detail["diagnostic"] == "REL-E0706"
    assert hierarchy.detail["max_depth"] == 4


def test_relation_assertions_reject_invalid_arguments() -> None:
    class MissingKey(Transform):
        bands = input(CohortBand)
        valid = output(CohortBand)

        def validate(self, band: CohortBand) -> CohortBand:
            return CohortBand.project(require_unique())

    class NonBooleanPredicate(Transform):
        bands = input(CohortBand)
        valid = output(CohortBand)

        def validate(self, band: CohortBand) -> CohortBand:
            return CohortBand.project(require_all(band.priority))

    with pytest.raises(TypeError, match="at least one key"):
        Compiler.frontend.compile()(MissingKey, materialize_schemas=False)
    with pytest.raises(TypeError, match="Boolean expression"):
        Compiler.frontend.compile()(NonBooleanPredicate, materialize_schemas=False)


def test_relation_reference_rejects_invalid_arguments() -> None:
    class BadReference(Transform):
        bands = input(CohortBand)
        valid = output(CohortBand)

        def validate(self, band: CohortBand) -> CohortBand:
            return CohortBand.project(require_reference(band.parent_id, object(), reference_key=band.band_id))

    class BadNullPolicy(Transform):
        bands = input(CohortBand)
        catalog = input(CohortBand)
        valid = output(CohortBand)

        def validate(self, band: CohortBand, catalog: CohortBand) -> CohortBand:
            return CohortBand.project(
                require_reference(band.parent_id, catalog, reference_key=catalog.band_id, nulls="skip")
            )

    with pytest.raises(TypeError, match="requires a Structure relation"):
        Compiler.frontend.compile()(BadReference, materialize_schemas=False)
    with pytest.raises(TypeError, match="'allow' or 'reject'"):
        Compiler.frontend.compile()(BadNullPolicy, materialize_schemas=False)


def test_parent_hierarchy_rejects_invalid_arguments() -> None:
    class MissingDepth(Transform):
        bands = input(CohortBand)
        valid = output(CohortBand)

        def validate(self, band: CohortBand) -> CohortBand:
            return CohortBand.project(
                require_parent_hierarchy(
                    band.band_id,
                    parent=band.parent_id,
                    order_by=band.priority,
                    max_depth=0,
                )
            )

    class NonFieldId(Transform):
        bands = input(CohortBand)
        valid = output(CohortBand)

        def validate(self, band: CohortBand) -> CohortBand:
            return CohortBand.project(
                require_parent_hierarchy(
                    1,
                    parent=band.parent_id,
                    order_by=band.priority,
                    max_depth=4,
                )
            )

    with pytest.raises(TypeError, match="positive integer literal"):
        Compiler.frontend.compile()(MissingDepth, materialize_schemas=False)
    with pytest.raises(TypeError, match="keys must be declared field references"):
        Compiler.frontend.compile()(NonFieldId, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(ValidateBands, materialize_schemas=False).lowered,
    )


def _lowered_reference() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(ValidateBandParents, materialize_schemas=False).lowered,
    )


def _lowered_hierarchy() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(ValidateBandHierarchy, materialize_schemas=False).lowered,
    )
