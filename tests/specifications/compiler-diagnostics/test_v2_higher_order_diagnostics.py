import pytest

import structure
from structure.app.dsl.api import compile_transform


class RawTags(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


class CleanTags(structure.Structure):
    tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)


class RawAttributes(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    attributes = structure.field(
        structure.Map(structure.String(), structure.String(), value_contains_null=True), nullable=True
    )


class CleanAttributes(structure.Structure):
    attributes = structure.field(
        structure.Map(structure.String(), structure.String(), value_contains_null=False), nullable=True
    )


def test_v2_array_transform_non_array_input_reports_actionable_diagnostic() -> None:
    @structure.transform
    class BadTransform(structure.Transform):
        rows = structure.input(RawTags)
        clean = structure.output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=structure.arr_transform(row.id, lambda tag: structure.lower(tag)))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadTransform)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadTransform.clean_tags")
    assert "arr_transform(...) requires an Array expression" in diagnostic.problem_text()


def test_v2_array_filter_non_boolean_callback_reports_actionable_diagnostic() -> None:
    @structure.transform
    class BadFilter(structure.Transform):
        rows = structure.input(RawTags)
        clean = structure.output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=structure.arr_filter(row.tags, lambda tag: structure.lower(structure.trim(tag))))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadFilter)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadFilter.clean_tags")
    assert "arr_filter(...) callback must return a Boolean expression" in diagnostic.problem_text()
    assert "Structure expression helpers" in diagnostic.use_text()


def test_v2_array_filter_python_boolean_callback_reports_helper_context() -> None:
    @structure.transform
    class BadCallback(structure.Transform):
        rows = structure.input(RawTags)
        clean = structure.output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=structure.arr_filter(row.tags, lambda tag: tag and tag.is_not_null()))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadCallback)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadCallback.clean_tags")
    assert "arr_filter(...) callback must stay inside Structure expression helpers" in diagnostic.problem_text()
    assert "Structure expressions cannot be used as Python booleans" in diagnostic.problem_text()


def test_v2_array_transform_untyped_callback_return_reports_actionable_diagnostic() -> None:
    @structure.transform
    class BadReturn(structure.Transform):
        rows = structure.input(RawTags)
        clean = structure.output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=structure.arr_transform(row.tags, lambda tag: object()))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadReturn)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadReturn.clean_tags")
    assert (
        "arr_transform(...) callback must return a typed Structure expression or literal" in diagnostic.problem_text()
    )


def test_v2_map_transform_non_map_input_reports_actionable_diagnostic() -> None:
    @structure.transform
    class BadTransform(structure.Transform):
        rows = structure.input(RawAttributes)
        clean = structure.output(CleanAttributes)

        def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
            return CleanAttributes(
                attributes=structure.map_transform_values(row.id, lambda key, value: structure.lower(value))
            )

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadTransform)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadTransform.clean_attributes")
    assert "map_transform_values(...) requires a Map expression" in diagnostic.problem_text()


def test_v2_map_filter_non_boolean_callback_reports_actionable_diagnostic() -> None:
    @structure.transform
    class BadFilter(structure.Transform):
        rows = structure.input(RawAttributes)
        clean = structure.output(CleanAttributes)

        def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
            return CleanAttributes(
                attributes=structure.map_filter(
                    row.attributes, lambda key, value: structure.lower(structure.trim(value))
                )
            )

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadFilter)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadFilter.clean_attributes")
    assert "map_filter(...) callback must return a Boolean expression" in diagnostic.problem_text()


def test_v2_map_filter_python_boolean_callback_reports_helper_context() -> None:
    @structure.transform
    class BadCallback(structure.Transform):
        rows = structure.input(RawAttributes)
        clean = structure.output(CleanAttributes)

        def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
            return CleanAttributes(attributes=structure.map_filter(row.attributes, lambda key, value: value and key))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadCallback)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadCallback.clean_attributes")
    assert "map_filter(...) callback must stay inside Structure expression helpers" in diagnostic.problem_text()
    assert "Structure expressions cannot be used as Python booleans" in diagnostic.problem_text()
