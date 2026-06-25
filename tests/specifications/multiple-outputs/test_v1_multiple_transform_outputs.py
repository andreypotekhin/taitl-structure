from typing import Any, cast

import pytest

from structure import String, Structure, Transform, field, input, lane, output, transform
from structure.app.dsl.api import compile_transform
from structure.app.runtime.api import StructureSession, TransformResult
from structure.app.target.pyspark.api import PySpark


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


def test_v1_multi_output_methods_write_source_order_lanes() -> None:
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

            where(cast(Any, row.customer_id).is_not_null())
            return Accepted(id=row.id, status="accepted")

        @transform(lane=accepted, output=accepted)
        def keep_accepted(self, row: Accepted) -> Accepted:
            from structure import where

            where(cast(Any, row.status) == "accepted")
            return Accepted(id=row.id, status=row.status)

        @transform(output=rejected)
        def reject(self, row: Normalized) -> Rejected:
            from structure import where

            where(cast(Any, row.customer_id).is_null())
            return Rejected(id=row.id, reason="missing customer")

    plan = compile_transform(RouteOrders)

    assert [(step.name, step.source, step.input_lane, step.output_lane) for step in plan.steps] == [
        ("normalize", "rows", "rows", "rows"),
        ("accept", "rows", "rows", "accepted"),
        ("keep_accepted", "accepted", "accepted", "accepted"),
        ("reject", "rows", "rows", "rejected"),
    ]
    assert [(item.name, item.source, item.schema) for item in plan.outputs] == [
        ("accepted", "accepted", Accepted),
        ("rejected", "rejected", Rejected),
    ]
    assert plan.steps[1].filters[0].kind == "is_not_null"
    assert plan.steps[2].filters[0].kind == "eq"
    assert plan.steps[3].filters[0].kind == "is_null"


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


def test_v1_single_field_declares_single_output_schema() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = compile_transform(PublishOrders)

    assert [item.name for item in plan.outputs] == ["published"]
    assert plan.output_schema is Published


def test_v1_transform_requires_output_field() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(Exception) as raised:
        compile_transform(PublishOrders)

    assert "PublishOrders declares no outputs" in str(raised.value)
    assert "name = output(Schema)" in str(raised.value)


def test_v1_transform_requires_input_field() -> None:
    @transform
    class PublishOrders(Transform):
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(Exception) as raised:
        compile_transform(PublishOrders)

    assert "PublishOrders declares no inputs" in str(raised.value)
    assert "name = input(Schema)" in str(raised.value)


def test_v1_class_to_option_is_rejected() -> None:
    with pytest.raises(TypeError) as raised:

        @transform(to=Published)
        class PublishOrders(Transform):
            rows = input(Raw)
            published = output(Published)

    assert "unknown class option(s): to" in str(raised.value)


def test_v1_method_input_selects_declared_input_when_schema_is_ambiguous() -> None:
    @transform
    class NormalizeOrders(Transform):
        external = input(Raw)
        internal = input(Raw)
        normalized = output(Normalized)

        @transform(input=external)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

    plan = compile_transform(NormalizeOrders)

    steps = [(step.name, step.source, step.source_scope, step.input_lane, step.output_lane) for step in plan.steps]
    assert steps == [
        ("normalize", "external", "external", "external", "external"),
    ]


def test_v1_method_input_selects_declared_input_before_writing_output_lane() -> None:
    @transform
    class AcceptOrders(Transform):
        external = input(Raw)
        internal = input(Raw)
        accepted = output(Accepted)

        @transform(input=external, output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

    plan = compile_transform(AcceptOrders)

    assert [(step.name, step.source, step.input_lane, step.output_lane) for step in plan.steps] == [
        ("accept", "external", "external", "accepted"),
    ]
    assert [(item.name, item.source, item.schema) for item in plan.outputs] == [
        ("accepted", "accepted", Accepted),
    ]


def test_v1_declared_lane_is_collected_and_written_from_input() -> None:
    @transform
    class NormalizeOrders(Transform):
        rows = input(Raw)
        normalized = lane(Normalized)
        published = output(Published)

        @transform(input=rows, output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @transform(lane=normalized, output=published)
        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    plan = compile_transform(NormalizeOrders)

    assert list(NormalizeOrders._structure_lanes) == ["normalized"]
    assert [(step.name, step.source, step.input_lane, step.output_lane) for step in plan.steps] == [
        ("normalize", "rows", "rows", "normalized"),
        ("publish", "normalized", "normalized", "published"),
    ]
    assert [(item.name, item.source, item.schema) for item in plan.outputs] == [
        ("published", "published", Published),
    ]


def test_v1_declared_lane_can_update_itself() -> None:
    @transform
    class NormalizeOrders(Transform):
        rows = input(Raw)
        normalized = lane(Normalized)
        published = output(Published)

        @transform(input=rows, output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @transform(lane=normalized)
        def keep_normalized(self, row: Normalized) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @transform(lane=normalized, output=published)
        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    assert [
        (step.name, step.source, step.input_lane, step.output_lane) for step in compile_transform(NormalizeOrders).steps
    ] == [
        ("normalize", "rows", "rows", "normalized"),
        ("keep_normalized", "normalized", "normalized", "normalized"),
        ("publish", "normalized", "normalized", "published"),
    ]


def test_v1_ambiguous_input_schema_requires_method_input() -> None:
    @transform
    class NormalizeOrders(Transform):
        external = input(Raw)
        internal = input(Raw)
        normalized = output(Normalized)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

    with pytest.raises(Exception) as raised:
        compile_transform(NormalizeOrders)

    assert "Cannot deduce input for schema Raw" in str(raised.value)
    assert "@transform(input=that_input)" in str(raised.value)


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

    assert "has no explicit transform method" in str(raised.value)


def test_v1_branch_input_must_be_available_earlier_in_source_order() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)
        rejected = output(Rejected)

        @transform(lane=accepted, output=rejected)
        def reject_from_missing_lane(self, row: Accepted) -> Rejected:
            return Rejected(id=row.id, reason="missing customer")

        @transform(output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

    with pytest.raises(Exception) as raised:
        compile_transform(RouteOrders)

    assert "Lane accepted is not available yet" in str(raised.value)


def test_v1_branch_input_schema_must_match_current_lane_schema() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)

        @transform(output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @transform(lane=accepted, output=accepted)
        def wrong_schema(self, row: Rejected) -> Accepted:
            return Accepted(id=row.id, status="accepted")

    with pytest.raises(Exception) as raised:
        compile_transform(RouteOrders)

    assert "lane accepted currently carries Accepted" in str(raised.value)


def test_v1_output_method_return_schema_must_match_output_lane_schema() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)

        @transform(output=accepted)
        def accept(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(Exception) as raised:
        compile_transform(RouteOrders)

    assert "returns Published, not Accepted" in str(raised.value)


def test_v1_output_lane_method_input_is_rejected() -> None:
    with pytest.raises(TypeError) as raised:

        @transform
        class RouteOrders(Transform):
            rows = input(Raw)
            accepted = output(Accepted)

            @transform(input=accepted)
            def accept(self, row: Raw) -> Accepted:
                return Accepted(id=row.id, status="accepted")

    assert "@transform(inputs=...) requires input(...) declarations" in str(raised.value)


def test_v1_generated_pyspark_uses_per_lane_step_sources() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)
        rejected = output(Rejected)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @transform(output=accepted)
        def accept(self, row: Normalized) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @transform(lane=accepted, output=accepted)
        def keep_accepted(self, row: Accepted) -> Accepted:
            return Accepted(id=row.id, status=row.status)

        @transform(output=rejected)
        def reject(self, row: Normalized) -> Rejected:
            return Rejected(id=row.id, reason="missing customer")

    recipe = PySpark.plan.lower()(compile_transform(RouteOrders))
    text = PySpark.render.transform()(
        recipe,
        source_transform="tests.specifications.multiple_outputs.RouteOrders",
        runtime_module="testing.runtime",
        schema_modules={
            Raw: "testing.schemas",
            Normalized: "testing.schemas",
            Accepted: "testing.schemas",
            Rejected: "testing.schemas",
        },
    )

    assert [step.source for step in recipe.steps] == ["rows", "rows", "accepted", "rows"]
    assert "        # Subtransform: accept\n        accepted = rows.alias(\"normalized\")" in text
    assert "        # Subtransform: keep_accepted\n        accepted = accepted.alias(\"accepted\")" in text
    assert "        # Subtransform: reject\n        rejected = rows.alias(\"normalized\")" in text
    assert 'return TransformResult({"accepted": accepted, "rejected": rejected}, single=False)' in text


def test_v1_online_executor_result_wraps_single_output_df() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    session = StructureSession(schema_types=_FakeTypes, online_executor=lambda **kwargs: "df")

    result = PublishOrders(rows="rows").run(session)

    assert isinstance(result, TransformResult)
    assert result.df == "df"
    assert result.published == "df"
    assert result["published"] == "df"


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
