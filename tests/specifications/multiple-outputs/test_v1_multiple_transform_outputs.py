from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.core.runtime.api import StructureSession, TransformResult
from structure.plugin.pyspark import *
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def _analysis(transform, **settings):
    return Compiler.frontend.compile()(transform, materialize_schemas=False, **settings).analysis


def _recipe(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False).lowered


class Raw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=True)


class Normalized(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=True)


class Accepted(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class Rejected(Schema):
    id = string(nullable=False)
    reason = string(nullable=False)


class Published(Schema):
    id = string(nullable=False)


def test_v1_multi_output_methods_write_source_order_lanes() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted_lane = lane(Accepted)
        accepted = output(Accepted)
        rejected = output(Rejected)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(output=accepted_lane)
        def accept(self, row: Normalized) -> Accepted:
            where(cast(Any, row.customer_id).is_not_null())
            return Accepted(id=row.id, status="accepted")

        @step(input=accepted_lane, output=accepted)
        def keep_accepted(self, row: Accepted) -> Accepted:
            where(cast(Any, row.status) == "accepted")
            return Accepted(id=row.id, status=row.status)

        @step(output=rejected)
        def reject(self, row: Normalized) -> Rejected:
            where(cast(Any, row.customer_id).is_null())
            return Rejected(id=row.id, reason="missing customer")

    plan = _analysis(RouteOrders)

    assert [(step.name, step.source, step.input_lane, step.output_lane) for step in plan.steps] == [
        ("normalize", "rows", "rows", "rows"),
        ("accept", "rows", "rows", "accepted_lane"),
        ("keep_accepted", "accepted_lane", "accepted_lane", "accepted"),
        ("reject", "rows", "rows", "rejected"),
    ]
    assert [(item.name, item.source, item.schema) for item in plan.outputs] == [
        ("accepted", "accepted", Accepted),
        ("rejected", "rejected", Rejected),
    ]
    assert cast(PySparkStepBody, plan.steps[1].plugin_body).filters[0].kind == "is_not_null"
    assert cast(PySparkStepBody, plan.steps[2].plugin_body).filters[0].kind == "eq"
    assert cast(PySparkStepBody, plan.steps[3].plugin_body).filters[0].kind == "is_null"


def test_v1_single_field_output_does_not_need_terminal_binding() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        out = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = _analysis(PublishOrders)

    assert [output.name for output in plan.outputs] == ["out"]
    assert plan.output_schema is Published


def test_v1_single_field_declares_single_output_schema() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = _analysis(PublishOrders)

    assert [item.name for item in plan.outputs] == ["published"]
    assert plan.output_schema is Published


def test_v1_transform_requires_output_field() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(Exception) as raised:
        _analysis(PublishOrders)

    assert "PublishOrders declares no outputs" in str(raised.value)
    assert "name = output(Schema)" in str(raised.value)


def test_v1_transform_requires_input_field() -> None:
    @transform
    class PublishOrders(Transform):
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(Exception) as raised:
        _analysis(PublishOrders)

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

        @step(input=external)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

    plan = _analysis(NormalizeOrders)

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

        @step(input=external, output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

    plan = _analysis(AcceptOrders)

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

        @step(input=rows, output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(input=normalized, output=published)
        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    plan = _analysis(NormalizeOrders)

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

        @step(input=rows, output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(input=normalized)
        def keep_normalized(self, row: Normalized) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(input=normalized, output=published)
        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    assert [
        (step.name, step.source, step.input_lane, step.output_lane) for step in _analysis(NormalizeOrders).steps
    ] == [
        ("normalize", "rows", "rows", "normalized"),
        ("keep_normalized", "normalized", "normalized", "normalized"),
        ("publish", "normalized", "normalized", "published"),
    ]


def test_v1_method_input_declaration_is_shadowed_by_existing_lane() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(input=rows)
        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    plan = _analysis(PublishOrders)

    assert [(step.name, step.source, step.input_lane, step.output_lane, step.input_schema) for step in plan.steps] == [
        ("normalize", "rows", "rows", "rows", Raw),
        ("publish", "rows", "rows", "rows", Normalized),
    ]
    assert [(item.name, item.source, item.schema) for item in plan.outputs] == [
        ("published", "rows", Published),
    ]


def test_v1_input_selector_reads_original_input_after_shadowing() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(input=input(rows), output=output(published))
        def publish_raw(self, row: Raw) -> Published:
            return Published(id=row.id)

    plan = _analysis(PublishOrders)

    assert [(step.name, step.source, step.input_lane, step.output_lane, step.input_schema) for step in plan.steps] == [
        ("normalize", "rows", "rows", "rows", Raw),
        ("publish_raw", "input:rows", "rows", "published", Raw),
    ]

    text = PySpark.render.transform()(
        PySpark.compiler.lower()(plan),
        source_transform="tests.specifications.multiple_outputs.PublishOrders",
        runtime_module="testing.runtime",
        schema_modules={
            Raw: "testing.schemas",
            Normalized: "testing.schemas",
            Published: "testing.schemas",
        },
    )

    assert "        _input_rows = rows" in text
    assert "        published = _input_rows.alias(\"raw\")" in text


def test_v1_lane_selector_requires_available_lane() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        @step(input=lane(rows), output=published)
        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(Exception, match="Lane rows is not available yet"):
        _analysis(PublishOrders)


def test_v1_lane_and_output_selectors_bind_inout_roles() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        @step(inout=input(rows) | lane(rows))
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(inout=lane(rows) | output(published))
        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    assert [
        (step.name, step.source, step.input_lane, step.output_lane, step.input_schema)
        for step in _analysis(PublishOrders).steps
    ] == [
        ("normalize", "input:rows", "rows", "rows", Raw),
        ("publish", "rows", "rows", "published", Normalized),
    ]


def test_v1_final_output_materializes_named_result_from_implicit_lane() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    text = PySpark.render.transform()(
        _recipe(PublishOrders),
        source_transform="tests.specifications.multiple_outputs.PublishOrders",
        runtime_module="testing.runtime",
        schema_modules={
            Raw: "testing.schemas",
            Normalized: "testing.schemas",
            Published: "testing.schemas",
        },
    )

    assert "        # Step method: published\n        published = rows.alias(\"published\")" in text
    assert '        assert_schema(published, PUBLISHED_SCHEMA, name="Published", mode="strict")' in text
    assert (
        'return TransformResult({"published": published}, single=True, schema={"published": PUBLISHED_SCHEMA})' in text
    )


def test_v1_inout_pipe_binds_source_and_output() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        normalized = lane(Normalized)
        published = output(Published)

        @step(inout=rows | normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(inout=normalized | published)
        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id)

    assert [(step.name, step.source, step.output_lane) for step in _analysis(PublishOrders).steps] == [
        ("normalize", "rows", "normalized"),
        ("publish", "normalized", "published"),
    ]


def test_v1_inout_pipe_accepts_multiple_outputs() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)
        rejected = output(Rejected)

        @step(inout=rows | [accepted, rejected])
        def route(self, row: Raw) -> tuple[Accepted, Rejected]:
            return Accepted(id=row.id, status="accepted"), Rejected(id=row.id, reason="missing customer")

    assert [item.lane for item in _analysis(RouteOrders).steps[0].results] == ["accepted", "rejected"]


def test_v1_inout_pipe_accepts_multiple_inputs() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        normalized = lane(Normalized)
        published = output(Published)

        @step(input=rows, output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(inout=[normalized, rows] | published)
        def publish(self, row: Normalized, raw: Raw) -> Published:
            return Published(id=row.id)

    assert [(item.parameter, item.source) for item in _analysis(PublishOrders).steps[1].inputs] == [
        ("row", "normalized"),
        ("raw", "rows"),
    ]


def test_v1_inout_pipe_rejects_multiple_inputs_and_outputs() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        normalized = lane(Normalized)
        accepted = output(Accepted)
        rejected = output(Rejected)

        with pytest.raises(TypeError):
            cast(Any, [rows, normalized]) | [accepted, rejected]


def test_v1_method_recycles_plural_and_lane_parameters() -> None:
    with pytest.raises(TypeError, match="inputs, outputs were recycled"):

        @transform
        class RouteOrders(Transform):
            rows = input(Raw)
            accepted = output(Accepted)

            @step(inputs=[rows], outputs=[accepted])
            def accept(self, row: Raw) -> Accepted:
                return Accepted(id=row.id, status="accepted")

    with pytest.raises(TypeError, match="lane were recycled"):

        @transform
        class ContinueOrders(Transform):
            rows = input(Raw)
            normalized = lane(Normalized)
            published = output(Published)

            @step(input=rows, output=normalized)
            def normalize(self, row: Raw) -> Normalized:
                return Normalized(id=row.id, customer_id=row.customer_id)

            @step(lane=normalized, output=published)
            def publish(self, row: Normalized) -> Published:
                return Published(id=row.id)


def test_v1_ambiguous_input_schema_requires_method_input() -> None:
    @transform
    class NormalizeOrders(Transform):
        external = input(Raw)
        internal = input(Raw)
        normalized = output(Normalized)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

    with pytest.raises(Exception) as raised:
        _analysis(NormalizeOrders)

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
        _analysis(RouteOrders)

    assert "Cannot deduce final output accepted" in str(raised.value)


def test_v1_branch_input_must_be_available_earlier_in_source_order() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted_lane = lane(Accepted)
        accepted = output(Accepted)
        rejected = output(Rejected)

        @step(input=accepted_lane, output=rejected)
        def reject_from_missing_lane(self, row: Accepted) -> Rejected:
            return Rejected(id=row.id, reason="missing customer")

        @step(output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

    with pytest.raises(Exception) as raised:
        _analysis(RouteOrders)

    assert "Lane accepted_lane is not available yet" in str(raised.value)


def test_v1_branch_input_schema_must_match_current_lane_schema() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted_lane = lane(Accepted)
        accepted = output(Accepted)

        @step(output=accepted_lane)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @step(input=accepted_lane, output=accepted)
        def wrong_schema(self, row: Rejected) -> Accepted:
            return Accepted(id=row.id, status="accepted")

    with pytest.raises(Exception) as raised:
        _analysis(RouteOrders)

    assert "lane accepted_lane currently carries Accepted" in str(raised.value)


def test_v1_output_method_return_schema_must_match_output_lane_schema() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)

        @step(output=accepted)
        def accept(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(Exception) as raised:
        _analysis(RouteOrders)

    assert "returns Published, not Accepted" in str(raised.value)


def test_v1_produced_output_can_be_used_as_a_step_input_by_default() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)

        @step(output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @step(input=accepted, output=accepted)
        def revise(self, row: Accepted) -> Accepted:
            return Accepted(id=row.id, status=row.status)

    plan = _analysis(RouteOrders)
    assert [(step.name, step.source, step.input_lane, step.output_lane) for step in plan.steps] == [
        ("accept", "rows", "rows", "accepted"),
        ("revise", "accepted", "accepted", "accepted"),
    ]


def test_v1_output_to_input_can_be_disabled() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)

        @step(output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @step(input=accepted, output=accepted)
        def revise(self, row: Accepted) -> Accepted:
            return Accepted(id=row.id, status=row.status)

    with pytest.raises(Exception) as raised:
        _analysis(RouteOrders, allow_output_to_input=False)

    assert "cannot be used as a step input" in str(raised.value)


def test_v1_output_reassignment_can_be_disabled_independently() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)

        @step(output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @step(input=accepted, output=accepted)
        def revise(self, row: Accepted) -> Accepted:
            return Accepted(id=row.id, status=row.status)

    with pytest.raises(Exception) as raised:
        _analysis(RouteOrders, allow_to_reassign_output=False)

    assert "assigned more than once" in str(raised.value)


def test_v1_bare_schema_inference_can_select_a_produced_output() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted = output(Accepted)
        rejected = output(Rejected)

        @step(output=accepted)
        def accept(self, row: Raw) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @step(output=rejected)
        def reject(self, row: Accepted) -> Rejected:
            return Rejected(id=row.id, reason="not accepted")

    plan = _analysis(RouteOrders)
    assert plan.steps[1].source == "accepted"


def test_v1_input_declaration_method_output_is_rejected() -> None:
    with pytest.raises(TypeError) as raised:

        @transform
        class RouteOrders(Transform):
            rows = input(Raw)
            accepted = output(Accepted)

            @step(output=rows)
            def accept(self, row: Raw) -> Accepted:
                return Accepted(id=row.id, status="accepted")

    assert "@step(output=...) requires lane(...) or output(...) declarations" in str(raised.value)


def test_v1_generated_pyspark_uses_per_lane_step_sources() -> None:
    @transform
    class RouteOrders(Transform):
        rows = input(Raw)
        accepted_lane = lane(Accepted)
        accepted = output(Accepted)
        rejected = output(Rejected)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, customer_id=row.customer_id)

        @step(output=accepted_lane)
        def accept(self, row: Normalized) -> Accepted:
            return Accepted(id=row.id, status="accepted")

        @step(input=accepted_lane, output=accepted)
        def keep_accepted(self, row: Accepted) -> Accepted:
            return Accepted(id=row.id, status=row.status)

        @step(output=rejected)
        def reject(self, row: Normalized) -> Rejected:
            return Rejected(id=row.id, reason="missing customer")

    recipe = _recipe(RouteOrders)
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

    assert [step.source for step in recipe.steps] == ["rows", "rows", "accepted_lane", "rows"]
    assert "        # Step method: accept\n        accepted_lane = rows.alias(\"normalized\")" in text
    assert "        # Step method: keep_accepted\n        accepted = accepted_lane.alias(\"accepted\")" in text
    assert "        # Step method: reject\n        rejected = rows.alias(\"normalized\")" in text
    assert "return TransformResult(" in text
    assert '"accepted": accepted' in text
    assert '"rejected": rejected' in text
    assert "single=False" in text
    assert '"accepted": ACCEPTED_SCHEMA' in text
    assert '"rejected": REJECTED_SCHEMA' in text


def test_v1_online_executor_result_wraps_single_output() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    session = StructureSession(schema_types=_FakeTypes, online_executor=lambda **kwargs: "df")

    result = PublishOrders(rows="rows").run(session)

    assert isinstance(result, TransformResult)
    assert result.published == "df"
    assert result["published"] == "df"
    assert result.schema.published == [("id", "string", False)]
    assert result.schema["published"] == [("id", "string", False)]


def test_v1_online_executor_preserves_single_field_output() -> None:
    @transform
    class PublishOrders(Transform):
        rows = input(Raw)
        out = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    session = StructureSession(schema_types=_FakeTypes, online_executor=lambda **kwargs: "df")

    result = PublishOrders(rows="rows").run(session)

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
