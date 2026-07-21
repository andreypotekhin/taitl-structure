from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def _compile(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def _body(transform) -> PySparkStepBody:
    return cast(PySparkStepBody, _compile(transform).analysis.steps[0].plugin_body)


class Raw(Schema):
    id = string(nullable=False)
    status = string(nullable=True)
    amount = string(nullable=False)
    count = integer(nullable=False)


class Published(Schema):
    id = string(nullable=False)
    status = string(nullable=True)


class Identity(Schema):
    id = string(nullable=False)


class Counted(Schema):
    count = long(nullable=False)


class Money(Schema):
    amount = decimal(12, 2, nullable=False)
    count = long(nullable=False)


class Customer(Schema):
    id = string(nullable=False)
    name = string(nullable=True)


def test_return_project_to_schema_copies_same_name_fields() -> None:
    """I can narrow a row to a target schema without repeating field names."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return project(row, Published)

    body = _body(Publish)

    assert [assignment.field.name for assignment in body.projection] == ["id", "status"]
    assert [cast(Any, assignment.expression.data)["field"] for assignment in body.projection] == [
        "id",
        "status",
    ]


def test_return_project_to_field_list_validates_source_fields() -> None:
    """I can narrow a row by listing the input fields I want to keep."""

    @transform
    class KeepIdentity(Transform):
        rows = input(Raw)
        identity = output(Identity)

        def publish(self, row: Raw) -> Identity:
            return project(row, ["id"])

    body = _body(KeepIdentity)

    assert [assignment.field.name for assignment in body.projection] == ["id"]


def test_project_source_argument_removes_multiple_parameter_ambiguity() -> None:
    """I can choose the source row explicitly when a step method has multiple schema parameters."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        customers = input(Customer)
        published = output(Published)

        def publish(self, row: Raw, customer: Customer) -> Published:
            lookup_join(customer, on=customer.id == row.id)
            return project(row, Published)

    body = _body(Publish)

    assert [cast(Any, assignment.expression.data)["scope"] for assignment in body.projection] == [
        "row",
        "row",
    ]


def test_where_project_shortcut_records_filter_and_projection() -> None:
    """I can use where(...).project(...) for compact filtered projection."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return cast(Published, where(cast(Any, row.status).is_not_null()).project(row, Published))

    body = _body(Publish)

    assert len(body.filters) == 1
    assert [assignment.field.name for assignment in body.projection] == ["id", "status"]


def test_generated_projection_narrowing_uses_select_not_drop() -> None:
    """Generated PySpark keeps projection narrowing optimizer-visible and deterministic."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return cast(Published, where(cast(Any, row.status).is_not_null()).project(row, Published))

    recipe = cast(PySparkExecutionPlan, _compile(Publish).lowered)
    text = PySpark.render.transform()(
        recipe,
        source_transform="tests.projection.Publish",
        runtime_module="tests.generated.runtime.schema_assert",
        schema_modules={
            Raw: "tests.generated.schemas.order",
            Published: "tests.generated.schemas.order",
        },
    )

    assert "rows = rows.select(" in text
    assert 'F.col("raw.id")' in text
    assert 'F.col("raw.status")' in text
    assert ".drop(" not in text


def test_schema_project_copies_fields_and_allows_overrides() -> None:
    """I can copy same-name source fields and override the fields that need adjustment."""

    @transform
    class Normalize(Transform):
        rows = input(Raw)
        money = output(Money)

        def normalize(self, row: Raw) -> Money:
            return Money.project(row)(amount=coalesce(to_decimal(row.amount, precision=12, scale=2), 0))

    projection = {assignment.field.name: assignment.expression for assignment in _body(Normalize).projection}

    assert projection["amount"].kind == "call"
    assert cast(Any, projection["count"].data)["field"] == "count"


def test_schema_project_without_overrides_copies_fields() -> None:
    """I can return schema projection directly when every copied field is unchanged."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published.project(row)

    projection = {assignment.field.name: assignment.expression for assignment in _body(Publish).projection}

    assert cast(Any, projection["id"].data)["field"] == "id"
    assert cast(Any, projection["status"].data)["field"] == "status"


def test_projection_accepts_type_widening() -> None:
    """Projection accepts the same widening rules as ordinary schema construction."""

    @transform
    class Count(Transform):
        rows = input(Raw)
        counted = output(Counted)

        def count(self, row: Raw) -> Counted:
            return project(row, Counted)

    body = _body(Count)

    assert body.projection[0].field.name == "count"


def test_source_less_project_uses_driving_row_when_unambiguous() -> None:
    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return project(Published)

    body = _body(Publish)

    assert [assignment.field.name for assignment in body.projection] == ["id", "status"]


def test_project_field_list_rejects_unknown_source_field() -> None:
    @transform
    class BadProject(Transform):
        rows = input(Raw)
        identity = output(Identity)

        def publish(self, row: Raw) -> Identity:
            return project(row, ["missing"])

    with pytest.raises(StructureCompileError) as raised:
        _compile(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert "has no field" in raised.value.diagnostic.problem_text()


def test_project_field_list_rejects_duplicate_names() -> None:
    @transform
    class BadProject(Transform):
        rows = input(Raw)
        identity = output(Identity)

        def publish(self, row: Raw) -> Identity:
            return project(row, ["id", "id"])

    with pytest.raises(StructureCompileError) as raised:
        _compile(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "cannot repeat field names" in raised.value.diagnostic.problem_text()


def test_where_chain_does_not_add_returning_method() -> None:
    @transform
    class BadProject(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return cast(Any, where(cast(Any, row.status).is_not_null())).returning(
                Published(id=row.id, status=row.status)
            )

    with pytest.raises(StructureCompileError) as raised:
        _compile(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "returning" in raised.value.diagnostic.problem_text()


def test_project_field_list_must_cover_target_fields() -> None:
    @transform
    class BadProject(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return project(row, ["id"])

    with pytest.raises(StructureCompileError) as raised:
        _compile(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert "Published.status is not selected" in raised.value.diagnostic.problem_text()


def test_project_rejects_incompatible_same_name_field_unless_overridden() -> None:
    @transform
    class BadProject(Transform):
        rows = input(Raw)
        money = output(Money)

        def normalize(self, row: Raw) -> Money:
            return Money.project(row)()

    with pytest.raises(StructureCompileError) as raised:
        _compile(BadProject)

    assert raised.value.diagnostic.code == "SCHEMA-E0302"
    assert raised.value.diagnostic.context["field"] == "amount"
