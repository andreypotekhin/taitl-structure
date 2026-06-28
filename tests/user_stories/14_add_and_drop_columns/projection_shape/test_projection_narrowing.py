from typing import Any, cast

import pytest

from structure import (
    Decimal,
    Integer,
    Long,
    String,
    Structure,
    StructureCompileError,
    Transform,
    field,
    input,
    join_one,
    output,
    project,
    to_decimal,
    transform,
    where,
)
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class Raw(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=True)
    amount = field(String(), nullable=False)
    count = field(Integer(), nullable=False)


class Published(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=True)


class Identity(Structure):
    id = field(String(), nullable=False)


class Counted(Structure):
    count = field(Long(), nullable=False)


class Money(Structure):
    amount = field(Decimal(12, 2), nullable=False)
    count = field(Long(), nullable=False)


class Customer(Structure):
    id = field(String(), nullable=False)
    name = field(String(), nullable=True)


def test_return_project_to_schema_copies_same_name_fields() -> None:
    """I can narrow a row to a target schema without repeating field names."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return project(row, Published)

    plan = compile_transform(Publish)

    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id", "status"]
    assert [cast(Any, assignment.expression.data)["field"] for assignment in plan.steps[0].projection] == [
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

    plan = compile_transform(KeepIdentity)

    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id"]


def test_project_source_argument_removes_multiple_parameter_ambiguity() -> None:
    """I can choose the source row explicitly when a subtransform has multiple schema parameters."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        customers = input(Customer)
        published = output(Published)

        def publish(self, row: Raw, customer: Customer) -> Published:
            join_one(customer, on=customer.id == row.id)
            return project(row, Published)

    plan = compile_transform(Publish)

    assert [cast(Any, assignment.expression.data)["scope"] for assignment in plan.steps[0].projection] == [
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
            return where(cast(Any, row.status).is_not_null()).project(row, Published)

    plan = compile_transform(Publish)

    assert len(plan.steps[0].filters) == 1
    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id", "status"]


def test_generated_projection_narrowing_uses_select_not_drop() -> None:
    """Generated PySpark keeps projection narrowing optimizer-visible and deterministic."""

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return where(cast(Any, row.status).is_not_null()).project(row, Published)

    recipe = PySpark.plan.lower()(compile_transform(Publish))
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
            return Money.project(row)(amount=to_decimal(row.amount, precision=12, scale=2))

    plan = compile_transform(Normalize)
    projection = {assignment.field.name: assignment.expression for assignment in plan.steps[0].projection}

    assert projection["amount"].kind == "call"
    assert cast(Any, projection["count"].data)["field"] == "count"


def test_projection_accepts_type_widening() -> None:
    """Projection accepts the same widening rules as ordinary schema construction."""

    @transform
    class Count(Transform):
        rows = input(Raw)
        counted = output(Counted)

        def count(self, row: Raw) -> Counted:
            return project(row, Counted)

    plan = compile_transform(Count)

    assert plan.steps[0].projection[0].field.name == "count"


def test_source_less_project_reports_clear_diagnostic() -> None:
    @transform
    class BadProject(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return project(Published)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "source row first" in raised.value.diagnostic.problem_text()


def test_project_field_list_rejects_unknown_source_field() -> None:
    @transform
    class BadProject(Transform):
        rows = input(Raw)
        identity = output(Identity)

        def publish(self, row: Raw) -> Identity:
            return project(row, ["missing"])

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadProject)

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
        compile_transform(BadProject)

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
        compile_transform(BadProject)

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
        compile_transform(BadProject)

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
        compile_transform(BadProject)

    assert raised.value.diagnostic.code == "SCHEMA-E0302"
    assert raised.value.diagnostic.context["field"] == "amount"
