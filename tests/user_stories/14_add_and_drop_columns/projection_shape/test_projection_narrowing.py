from typing import Any, cast

import pytest

import structure
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class Raw(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    status = structure.field(structure.String(), nullable=True)
    amount = structure.field(structure.String(), nullable=False)
    count = structure.field(structure.Integer(), nullable=False)


class Published(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    status = structure.field(structure.String(), nullable=True)


class Identity(structure.Structure):
    id = structure.field(structure.String(), nullable=False)


class Counted(structure.Structure):
    count = structure.field(structure.Long(), nullable=False)


class Money(structure.Structure):
    amount = structure.field(structure.Decimal(12, 2), nullable=False)
    count = structure.field(structure.Long(), nullable=False)


class Customer(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    name = structure.field(structure.String(), nullable=True)


def test_return_project_to_schema_copies_same_name_fields() -> None:
    """I can narrow a row to a target schema without repeating field names."""

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return structure.project(row, Published)

    plan = compile_transform(Publish)

    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id", "status"]
    assert [cast(Any, assignment.expression.data)["field"] for assignment in plan.steps[0].projection] == [
        "id",
        "status",
    ]


def test_return_project_to_field_list_validates_source_fields() -> None:
    """I can narrow a row by listing the input fields I want to keep."""

    @structure.transform
    class KeepIdentity(structure.Transform):
        rows = structure.input(Raw)
        identity = structure.output(Identity)

        def publish(self, row: Raw) -> Identity:
            return structure.project(row, ["id"])

    plan = compile_transform(KeepIdentity)

    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id"]


def test_project_source_argument_removes_multiple_parameter_ambiguity() -> None:
    """I can choose the source row explicitly when a step method has multiple schema parameters."""

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        customers = structure.input(Customer)
        published = structure.output(Published)

        def publish(self, row: Raw, customer: Customer) -> Published:
            structure.lookup_join(customer, on=customer.id == row.id)
            return structure.project(row, Published)

    plan = compile_transform(Publish)

    assert [cast(Any, assignment.expression.data)["scope"] for assignment in plan.steps[0].projection] == [
        "row",
        "row",
    ]


def test_where_project_shortcut_records_filter_and_projection() -> None:
    """I can use where(...).project(...) for compact filtered projection."""

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return structure.where(cast(Any, row.status).is_not_null()).project(row, Published)

    plan = compile_transform(Publish)

    assert len(plan.steps[0].filters) == 1
    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id", "status"]


def test_generated_projection_narrowing_uses_select_not_drop() -> None:
    """Generated PySpark keeps projection narrowing optimizer-visible and deterministic."""

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return structure.where(cast(Any, row.status).is_not_null()).project(row, Published)

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

    @structure.transform
    class Normalize(structure.Transform):
        rows = structure.input(Raw)
        money = structure.output(Money)

        def normalize(self, row: Raw) -> Money:
            return Money.project(row)(amount=structure.to_decimal(row.amount, precision=12, scale=2))

    plan = compile_transform(Normalize)
    projection = {assignment.field.name: assignment.expression for assignment in plan.steps[0].projection}

    assert projection["amount"].kind == "call"
    assert cast(Any, projection["count"].data)["field"] == "count"


def test_schema_project_without_overrides_copies_fields() -> None:
    """I can return schema projection directly when every copied field is unchanged."""

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published.project(row)

    plan = compile_transform(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in plan.steps[0].projection}

    assert cast(Any, projection["id"].data)["field"] == "id"
    assert cast(Any, projection["status"].data)["field"] == "status"


def test_projection_accepts_type_widening() -> None:
    """Projection accepts the same widening rules as ordinary schema construction."""

    @structure.transform
    class Count(structure.Transform):
        rows = structure.input(Raw)
        counted = structure.output(Counted)

        def count(self, row: Raw) -> Counted:
            return structure.project(row, Counted)

    plan = compile_transform(Count)

    assert plan.steps[0].projection[0].field.name == "count"


def test_source_less_project_uses_driving_row_when_unambiguous() -> None:
    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return structure.project(Published)

    plan = compile_transform(Publish)

    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id", "status"]


def test_project_field_list_rejects_unknown_source_field() -> None:
    @structure.transform
    class BadProject(structure.Transform):
        rows = structure.input(Raw)
        identity = structure.output(Identity)

        def publish(self, row: Raw) -> Identity:
            return structure.project(row, ["missing"])

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert "has no field" in raised.value.diagnostic.problem_text()


def test_project_field_list_rejects_duplicate_names() -> None:
    @structure.transform
    class BadProject(structure.Transform):
        rows = structure.input(Raw)
        identity = structure.output(Identity)

        def publish(self, row: Raw) -> Identity:
            return structure.project(row, ["id", "id"])

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "cannot repeat field names" in raised.value.diagnostic.problem_text()


def test_where_chain_does_not_add_returning_method() -> None:
    @structure.transform
    class BadProject(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return cast(Any, structure.where(cast(Any, row.status).is_not_null())).returning(
                Published(id=row.id, status=row.status)
            )

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "returning" in raised.value.diagnostic.problem_text()


def test_project_field_list_must_cover_target_fields() -> None:
    @structure.transform
    class BadProject(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return structure.project(row, ["id"])

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadProject)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert "Published.status is not selected" in raised.value.diagnostic.problem_text()


def test_project_rejects_incompatible_same_name_field_unless_overridden() -> None:
    @structure.transform
    class BadProject(structure.Transform):
        rows = structure.input(Raw)
        money = structure.output(Money)

        def normalize(self, row: Raw) -> Money:
            return Money.project(row)()

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadProject)

    assert raised.value.diagnostic.code == "SCHEMA-E0302"
    assert raised.value.diagnostic.context["field"] == "amount"
