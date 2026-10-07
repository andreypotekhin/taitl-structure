from __future__ import annotations

from typing import cast

import pytest

from structure import Schema, Transform, input, output, sink, step
from structure.core.compiler.api import Compiler
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.runtime.session.model.SinkResult import SinkResult
from structure.core.runtime.session.model.TransformResult import TransformResult
from structure.plugin.pyspark import Sink, foreach, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Event(Schema):
    id = string(nullable=False)


class Alert(Schema):
    id = string(nullable=False)


class AlertWriter(Sink[Alert]):
    def process(self, row: object) -> None:
        pass


class OtherWriter(Sink[Alert]):
    def process(self, row: object) -> None:
        pass


class StreamingWriter(Sink[Alert]):
    def process(self, row: object) -> None:
        pass

    def open(self, partition_id: int, epoch_id: int) -> bool:
        return True

    def close(self, error: Exception | None) -> None:
        pass


class CallableWriter(Sink[Alert]):
    def process(self, row: object) -> None:
        pass

    def __call__(self, row: object) -> None:
        pass


class BatchFrame:
    isStreaming = False

    def foreach(self, function) -> None:
        self.function = function


class StreamBuilder:
    def foreach(self, writer):
        self.writer = writer
        return self


class StreamFrame:
    isStreaming = True

    def __init__(self) -> None:
        self.writeStream = StreamBuilder()


class PublishAlerts(Transform):
    events = input(Event)
    alerts = output(Alert)
    publish_alerts = sink(Alert)

    @step(output=alerts)
    def publish(self, event: Event) -> Alert:
        alert = Alert(id=event.id)
        foreach(alert, self.publish_alerts)
        return alert


def _compile(transform=PublishAlerts):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_foreach_binds_declared_writer_and_final_output() -> None:
    compiled = _compile()
    plan = cast(PySparkExecutionPlan, compiled.lowered)

    assert plan.sinks[0].name == "publish_alerts"
    assert plan.sinks[0].output == "alerts"
    assert plan.sinks[0].sink_module == __name__
    assert plan.sinks[0].sink_qualname == "Alert"
    assert plan.sinks[0].streaming is False
    assert compiled.analysis.steps[0].sinks[0].result_ordinal == 0


def test_foreach_requires_a_captured_declared_sink() -> None:
    class Undeclared(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            return Alert(id=event.id)

    assert _compile(Undeclared).lowered.sinks == ()


def test_foreach_rejects_intermediate_rows_without_final_output_binding() -> None:
    class Intermediate(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.publish_alerts)
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
        publish_alerts = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.publish_alerts)
            return alert

    assert _compile(PublishStream).lowered.sinks[0].streaming is True


def test_direct_sink_reference_resolves_same_type_schema_ambiguity() -> None:
    class TwoSinks(Transform):
        events = input(Event)
        alerts = output(Alert)
        audit = sink(Alert)
        notify = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.notify)
            return alert

    plan = _compile(TwoSinks).lowered
    assert [declared.name for declared in plan.sinks] == ["notify"]


def test_foreach_requires_a_declared_sink_reference() -> None:
    class MissingSink(Transform):
        events = input(Event)
        alerts = output(Alert)

        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.missing)
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(MissingSink)
    assert "no output 'missing'" in raised.value.diagnostic.problem_text()


def test_sink_requires_the_sink_role_base() -> None:
    class PlainWriter:
        def process(self, row: object) -> None:
            pass

    with pytest.raises(TypeError, match="Structure Schema"):
        sink(PlainWriter)


def test_sink_requires_a_concrete_process_implementation() -> None:
    with pytest.raises(TypeError, match="Structure Schema"):
        sink(Sink)


def test_step_cannot_bind_a_sink_parameter() -> None:
    with pytest.raises(TypeError, match="unknown method option.*sink"):
        step(output=output(Alert), sink=sink(Alert))(lambda self, alert: alert)


def test_sink_is_exported_without_importing_pyspark() -> None:
    import sys

    before = {name for name in sys.modules if name.startswith("pyspark")}
    from structure.plugin.pyspark import Sink as PublicSink

    assert PublicSink is Sink
    assert {name for name in sys.modules if name.startswith("pyspark")} == before


def test_sink_writer_method_calls_are_opaque_during_step_compilation() -> None:
    class CallWriterInStep(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            AlertWriter().process(event)
            return Alert(id=event.id)

    with pytest.raises(StructureCompileError) as raised:
        _compile(CallWriterInStep)
    assert raised.value.diagnostic.code == "DSL-E0405"


def test_foreach_rejects_a_row_schema_that_differs_from_the_sink() -> None:
    class WrongType(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        def publish(self, event: Event) -> Alert:
            foreach(event, self.publish_alerts)
            return Alert(id=event.id)

    with pytest.raises(StructureCompileError) as raised:
        _compile(WrongType)
    assert "received Event" in raised.value.diagnostic.problem_text()


def test_duplicate_sink_capture_is_a_foreach_diagnostic() -> None:
    class Duplicate(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.publish_alerts)
            foreach(alert, self.publish_alerts)
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(Duplicate)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "attached more than once" in raised.value.diagnostic.problem_text()


def test_foreach_requires_the_exact_row_value_returned_by_the_step() -> None:
    class Reconstructed(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.publish_alerts)
            return Alert(id=event.id)

    with pytest.raises(StructureCompileError) as raised:
        _compile(Reconstructed)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "does not reference a value returned by this step" in raised.value.diagnostic.problem_text()


def test_sink_name_cannot_collide_with_transform_result_members() -> None:
    class ReservedName(Transform):
        events = input(Event)
        alerts = output(Alert)
        schema = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.schema)
            return alert

    with pytest.raises(StructureCompileError) as raised:
        _compile(ReservedName)
    assert raised.value.diagnostic.code == "DSL-E0406"
    assert "cannot be exposed" in raised.value.diagnostic.problem_text()


def test_batch_sink_rejects_streaming_lifecycle_methods() -> None:
    class BatchWithStreamingWriter(Transform):
        events = input(Event)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.publish_alerts)
            return alert

    compiled = _compile(BatchWithStreamingWriter)
    assert compiled.lowered.sinks[0].kind == "row"


def test_streaming_sink_rejects_callable_writer() -> None:
    class CallableStream(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        publish_alerts = sink(Alert)

        @step(output=alerts)
        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach(alert, self.publish_alerts)
            return alert

    _compile(CallableStream)
    handoff = SinkResult(
        dataframe=StreamFrame(), schema=Alert, output="alerts", kind="row", name="publish_alerts"
    )
    with pytest.raises(TypeError, match="noncallable"):
        CallableWriter().write_stream(handoff)


def test_sink_writer_uses_class_first_batch_and_stream_apis() -> None:
    batch_frame = BatchFrame()
    batch_handoff = SinkResult(
        dataframe=batch_frame, schema=Alert, output="alerts", kind="row", name="publish_alerts"
    )
    writer = AlertWriter()
    writer.write(batch_handoff)
    assert batch_frame.function == writer.process

    stream_frame = StreamFrame()
    stream_handoff = SinkResult(
        dataframe=stream_frame, schema=Alert, output="alerts", kind="row", name="publish_alerts"
    )
    assert StreamingWriter().write_stream(stream_handoff) is stream_frame.writeStream
    assert stream_frame.writeStream.writer is not None


def test_row_sink_rejects_a_mismatched_writer_schema_and_batch_lifecycle() -> None:
    handoff = SinkResult(
        dataframe=BatchFrame(), schema=Alert, output="alerts", kind="row", name="publish_alerts"
    )

    class EventWriter(Sink[Event]):
        def process(self, row: object) -> None:
            pass

    with pytest.raises(TypeError, match="consumes Event, but sink 'publish_alerts' declares Alert"):
        EventWriter().write(handoff)
    with pytest.raises(TypeError, match=r"cannot use a writer that defines open\(\) or close\(\)"):
        StreamingWriter().write(handoff)


def test_result_exposes_read_only_handoff_without_changing_output_mapping() -> None:
    dataframe = object()
    result = TransformResult({"alerts": dataframe}, single=True)
    result._structure_with_sinks(
        {
            "publish_alerts": SinkResult(
                dataframe=dataframe, schema=Alert, output="alerts", kind="row", name="publish_alerts"
            )
        }
    )

    assert result.publish_alerts.dataframe is result.alerts
    assert result.publish_alerts.schema is Alert
    assert dict(result) == {"alerts": dataframe}
    assert len(result) == 1
    with pytest.raises(AttributeError, match="read-only"):
        result.publish_alerts = None
