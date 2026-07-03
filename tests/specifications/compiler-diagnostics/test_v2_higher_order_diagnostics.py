import pytest

from structure import (
    Array,
    Map,
    String,
    Structure,
    StructureCompileError,
    Transform,
    arr_filter,
    arr_transform,
    field,
    input,
    lower,
    map_filter,
    map_transform_values,
    output,
    transform,
    trim,
)
from structure.app.dsl.api import compile_transform


class RawTags(Structure):
    id = field(String(), nullable=False)
    tags = field(Array(String(), contains_null=False), nullable=True)


class CleanTags(Structure):
    tags = field(Array(String(), contains_null=False), nullable=True)


class RawAttributes(Structure):
    id = field(String(), nullable=False)
    attributes = field(Map(String(), String(), value_contains_null=True), nullable=True)


class CleanAttributes(Structure):
    attributes = field(Map(String(), String(), value_contains_null=False), nullable=True)


def test_v2_array_transform_non_array_input_reports_actionable_diagnostic() -> None:
    @transform
    class BadTransform(Transform):
        rows = input(RawTags)
        clean = output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=arr_transform(row.id, lambda tag: lower(tag)))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadTransform)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadTransform.clean_tags")
    assert "arr_transform(...) requires an Array expression" in diagnostic.problem_text()


def test_v2_array_filter_non_boolean_callback_reports_actionable_diagnostic() -> None:
    @transform
    class BadFilter(Transform):
        rows = input(RawTags)
        clean = output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=arr_filter(row.tags, lambda tag: lower(trim(tag))))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadFilter)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadFilter.clean_tags")
    assert "arr_filter(...) callback must return a Boolean expression" in diagnostic.problem_text()
    assert "Structure expression helpers" in diagnostic.use_text()


def test_v2_array_filter_python_boolean_callback_reports_helper_context() -> None:
    @transform
    class BadCallback(Transform):
        rows = input(RawTags)
        clean = output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=arr_filter(row.tags, lambda tag: tag and tag.is_not_null()))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadCallback)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadCallback.clean_tags")
    assert "arr_filter(...) callback must stay inside Structure expression helpers" in diagnostic.problem_text()
    assert "Structure expressions cannot be used as Python booleans" in diagnostic.problem_text()


def test_v2_array_transform_untyped_callback_return_reports_actionable_diagnostic() -> None:
    @transform
    class BadReturn(Transform):
        rows = input(RawTags)
        clean = output(CleanTags)

        def clean_tags(self, row: RawTags) -> CleanTags:
            return CleanTags(tags=arr_transform(row.tags, lambda tag: object()))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadReturn)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadReturn.clean_tags")
    assert "arr_transform(...) callback must return a typed Structure expression or literal" in diagnostic.problem_text()


def test_v2_map_transform_non_map_input_reports_actionable_diagnostic() -> None:
    @transform
    class BadTransform(Transform):
        rows = input(RawAttributes)
        clean = output(CleanAttributes)

        def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
            return CleanAttributes(attributes=map_transform_values(row.id, lambda key, value: lower(value)))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadTransform)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadTransform.clean_attributes")
    assert "map_transform_values(...) requires a Map expression" in diagnostic.problem_text()


def test_v2_map_filter_non_boolean_callback_reports_actionable_diagnostic() -> None:
    @transform
    class BadFilter(Transform):
        rows = input(RawAttributes)
        clean = output(CleanAttributes)

        def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
            return CleanAttributes(attributes=map_filter(row.attributes, lambda key, value: lower(trim(value))))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadFilter)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadFilter.clean_attributes")
    assert "map_filter(...) callback must return a Boolean expression" in diagnostic.problem_text()


def test_v2_map_filter_python_boolean_callback_reports_helper_context() -> None:
    @transform
    class BadCallback(Transform):
        rows = input(RawAttributes)
        clean = output(CleanAttributes)

        def clean_attributes(self, row: RawAttributes) -> CleanAttributes:
            return CleanAttributes(attributes=map_filter(row.attributes, lambda key, value: value and key))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadCallback)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.source.endswith("BadCallback.clean_attributes")
    assert "map_filter(...) callback must stay inside Structure expression helpers" in diagnostic.problem_text()
    assert "Structure expressions cannot be used as Python booleans" in diagnostic.problem_text()
