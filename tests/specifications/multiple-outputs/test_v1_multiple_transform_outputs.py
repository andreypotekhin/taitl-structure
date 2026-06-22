import pytest

from structure import String, Structure, Transform, field, input, output, transform
from structure.app.dsl.api import compile_transform
from structure.app.runtime.api import StructureSession, TransformResult


class Raw(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=True)


class Normalized(Structure):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=True)


class Accepted(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=False)


class Rejected(Structure):
    id = field(String(), nullable=False)
    reason = field(String(), nullable=False)


class Published(Structure):
    id = field(String(), nullable=False)


def test_v1_multi_output_methods_are_terminal_siblings() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)
        rejected = output(Rejected)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @transform(output=accepted)
        def accept(self, row: Normalized) -> Accepted:
            from structure import where

            where(row.customer_id.is_not_null())
            return Accepted(id=row.id, status="accepted")

        @transform(output=rejected)
        def reject(self, row: Normalized) -> Rejected:
            from structure import where

            where(row.customer_id.is_null())
            return Rejected(id=row.id, reason="missing customer")

    plan = compile_transform(RouteOrders)

    assert [step.name for step in plan.steps] == ["normalize"]
    assert [(item.name, item.source, item.schema) for item in plan.outputs] == [
        ("accepted", "normalize", Accepted),
        ("rejected", "normalize", Rejected),
    ]
    assert plan.outputs[0].filters[0].kind == "is_not_null"
    assert plan.outputs[1].filters[0].kind == "is_null"


def test_v1_single_field_output_does_not_need_terminal_binding() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        out = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = compile_transform(PublishOrders)

    assert [output.name for output in plan.outputs] == ["out"]
    assert plan.output_schema is Published


def test_v1_class_to_declares_single_output_schema() -> None:
    @transform(to=Published)
    class PublishOrders(Transform):
        rows = input(Raw)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = compile_transform(PublishOrders)

    assert [output.name for output in plan.outputs] == ["df"]
    assert plan.output_schema is Published


def test_v1_multi_output_requires_explicit_terminal_bindings() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)
        rejected = output(Rejected)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

    with pytest.raises(Exception) as raised:
        compile_transform(RouteOrders)

    assert "has no terminal transform method" in str(raised.value)


def test_v1_online_executor_result_wraps_single_output_df() -> None:
    @transform(to=Published)
    class PublishOrders(Transform):
        rows = input(Raw)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    session = StructureSession(schema_types=_FakeTypes, online_executor=lambda **kwargs: "df")

    result = PublishOrders(rows="rows").run(session)

    assert isinstance(result, TransformResult)
    assert result.df == "df"
    assert result["df"] == "df"


def test_v1_online_executor_preserves_single_field_output_alias() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        out = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    session = StructureSession(schema_types=_FakeTypes, online_executor=lambda **kwargs: "df")

    result = PublishOrders(rows="rows").run(session)

    assert result.df == "df"
    assert result.out == "df"


class _FakeTypes:

    @staticmethod
    def StructType(fields):
        return fields

    @staticmethod
    def StructField(name, dataType, nullable):
        return (name, dataType, nullable)

    @staticmethod
    def StringType():
        return "string"
