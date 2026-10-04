from __future__ import annotations

from typing import cast

import pytest

from structure import Schema, Transform, input, output, sink, special, step
from structure.core.compiler.api import Compiler
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.runtime.session.model.SinkResult import SinkResult
from structure.core.runtime.session.model.TransformResult import TransformResult
from structure.plugin.pyspark import foreach, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Event(Schema):
    id = string(nullable=False)


class Alert(Schema):
    id = string(nullable=False)


@special(type="opaque")
class AlertWriter:
    def process(self, row: object) -> None:
        pass


@special(type="opaque")
class OtherWriter:
    def process(self, row: object) -> None:
        pass


@special(type="opaque")
class StreamingWriter:
    def process(self, row: object) -> None:
        pass

    def open(self, partition_id: int, epoch_id: int) -> bool:
        return True

    def close(self, error: Exception | None) -> None:
        pass


@special(type="opaque")
class CallableWriter:
    def process(self, row: object) -> None:
        pass

    def __call__(self, row: object) -> None:
        pass


class PublishAlerts(Transform):
    events = input(Event)
    alerts = output(Alert)
    publish_alerts = sink(AlertWriter)

    @step(output=alerts)
    def publish(self, event: Event, sink: AlertWriter) -> Alert:
        alert = Alert(id=event.id)
        foreach(alert, sink)
        return alert


def _compile(transform=PublishAlerts):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_foreach_binds_declared_writer_and_final_output() -> None:
    compiled = _compile()
    plan = cast(PySparkExecutionPlan, compiled.lowered)

    assert plan.sinks[0].name == "publish_alerts"
    assert plan.sinks[0].output == "alerts"
    assert plan.sinks[0].writer_module == __name__
    assert plan.sinks[0].writer_qualname == "AlertWriter"
    assert plan.sinks[0].streaming is False
    assert compiled.analysis.steps[0].sinks[0].result_ordinal == 0


def test_foreach_requires_the_declared_sink_parameter() -> None:
    class Undeclared(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(AlertWriter)

        @step(output=alerts)
        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            return Alert(id=event.id)

    assert _compile(Undeclared).lowered.sinks == ()


def test_foreach_rejects_intermediate_rows_without_final_output_binding() -> None:
    class Intermediate(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(AlertWriter)

        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            return alert

        def finish(self, alert: Alert) -> Alert:
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(Intermediate)

    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "not a declared final output" in raised.value.diagnostic.problem_text()


def test_streaming_final_output_selects_streaming_writer_contract() -> None:
    class PublishStream(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        publish_alerts = sink(AlertWriter)

        @step(output=alerts)
        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            return alert

    assert _compile(PublishStream).lowered.sinks[0].streaming is True


def test_explicit_sink_binding_resolves_same_type_writer_ambiguity() -> None:
    class TwoSinks(Transform):
        events = input(Event)
        alerts = output(Alert)
        audit = sink(AlertWriter)
        notify = sink(AlertWriter)

        @step(output=alerts, sink=notify)
        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            return alert

    plan = _compile(TwoSinks).lowered
    assert [declared.name for declared in plan.sinks] == ["notify"]


def test_sink_parameter_requires_a_matching_declaration() -> None:
    class MissingSink(Transform):
        events = input(Event)
        alerts = output(Alert)

        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            return Alert(id=event.id)

    with pytest.raises(StructureCompileError) as raised:
        _compile(MissingSink)
    assert "Declare sink(WriterClass)" in raised.value.diagnostic.use_text()


def test_sink_parameter_rejects_a_wrong_writer_type() -> None:
    class WrongType(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(AlertWriter)

        def publish(self, event: Event, sink: OtherWriter) -> Alert:
            return Alert(id=event.id)

    with pytest.raises(StructureCompileError) as raised:
        _compile(WrongType)
    assert "matching declarations: none" in raised.value.diagnostic.problem_text()


def test_duplicate_sink_capture_is_a_foreach_diagnostic() -> None:
    class Duplicate(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(AlertWriter)

        @step(output=alerts)
        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            foreach(alert, sink)
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(Duplicate)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "attached more than once" in raised.value.diagnostic.problem_text()


def test_foreach_requires_the_exact_row_value_returned_by_the_step() -> None:
    class Reconstructed(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(AlertWriter)

        @step(output=alerts)
        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            return Alert(id=event.id)

    with pytest.raises(StructureCompileError) as raised:
        _compile(Reconstructed)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "does not reference a value returned by this step" in raised.value.diagnostic.problem_text()


def test_sink_name_cannot_collide_with_transform_result_members() -> None:
    class ReservedName(Transform):
        events = input(Event)
        alerts = output(Alert)
        schema = sink(AlertWriter)

        @step(output=alerts)
        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(ReservedName)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "cannot be exposed" in raised.value.diagnostic.problem_text()


def test_batch_sink_rejects_streaming_lifecycle_methods() -> None:
    class BatchWithStreamingWriter(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(StreamingWriter)

        @step(output=alerts)
        def publish(self, event: Event, sink: StreamingWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(BatchWithStreamingWriter)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "streaming open/close" in raised.value.diagnostic.problem_text()


def test_streaming_sink_rejects_callable_writer() -> None:
    class CallableStream(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        publish_alerts = sink(CallableWriter)

        @step(output=alerts)
        def publish(self, event: Event, sink: CallableWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, sink)
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(CallableStream)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "writer CallableWriter is callable" in raised.value.diagnostic.problem_text()


def test_result_exposes_read_only_handoff_without_changing_output_mapping() -> None:
    dataframe = object()
    result = TransformResult({"alerts": dataframe}, single=True)
    result._structure_with_sinks({"publish_alerts": SinkResult(dataframe=dataframe, writer=AlertWriter)})

    assert result.publish_alerts.dataframe is result.alerts
    assert result.publish_alerts.writer is AlertWriter
    assert dict(result) == {"alerts": dataframe}
    assert len(result) == 1
    with pytest.raises(AttributeError, match="read-only"):
        result.publish_alerts = None
