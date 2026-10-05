from __future__ import annotations

from typing import cast

import pytest

from structure import Schema, Transform, input, output, sink, step
from structure.core.compiler.api import Compiler
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.plugin.pyspark import Sink, foreach_batch, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Event(Schema):
    id = string(nullable=False)


class Alert(Schema):
    id = string(nullable=False)


class AlertMessage(Schema):
    id = string(nullable=False)
    payload = string(nullable=False)


class AlertWriter(Sink):
    def process(self, row: object) -> None:
        pass


class PublishAlerts(Transform):
    events = input(Event, streaming=True)
    alerts = output(Alert)
    send_alerts = sink(AlertMessage)

    @step(output=alerts)
    def publish(self, event: Event, sink: AlertMessage) -> Alert:
        alert = Alert(id=event.id)
        foreach_batch(alert, sink)
        return alert


def _compile(transform=PublishAlerts):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_foreach_batch_binds_schema_sink_to_streaming_output() -> None:
    compiled = _compile()
    plan = cast(PySparkExecutionPlan, compiled.lowered)

    assert len(plan.steps) == 1
    assert plan.steps[0].name == "publish"
    assert plan.sinks[0].name == "send_alerts"
    assert plan.sinks[0].kind == "batch"
    assert plan.sinks[0].output == "alerts"
    assert plan.sinks[0].sink_module == __name__
    assert plan.sinks[0].sink_qualname == "AlertMessage"
    assert plan.sinks[0].streaming is True
    assert compiled.analysis.steps[0].sinks[0].result_ordinal == 0


def test_sink_effect_step_binds_previous_final_output_without_result_frame() -> None:
    class PublishInTwoSteps(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        send_alerts = sink(AlertMessage)

        def alert(self, event: Event) -> Alert:
            return Alert(id=event.id)

        def publish_batch(self, alert: Alert, sink: AlertMessage) -> AlertMessage:
            return foreach_batch(alert, sink)

    compiled = _compile(PublishInTwoSteps)
    plan = cast(PySparkExecutionPlan, compiled.lowered)

    assert [step.name for step in plan.steps] == ["alert"]
    assert plan.sinks[0].output == "alerts"


def test_foreach_batch_requires_explicit_sink_parameter() -> None:
    class MissingSinkParameter(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        send_alerts = sink(AlertMessage)

        def publish(self, event: Event) -> Alert:
            alert = Alert(id=event.id)
            foreach_batch(alert)  # type: ignore[call-arg]
            return alert

    with pytest.raises(StructureCompileError):
        _compile(MissingSinkParameter)


def test_foreach_batch_rejects_row_writer_sink() -> None:
    class WrongHelper(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        send_alerts = sink(AlertWriter)

        def publish(self, event: Event, sink: AlertWriter) -> Alert:
            alert = Alert(id=event.id)
            foreach_batch(alert, sink)
            return alert

    with pytest.raises(StructureCompileError):
        _compile(WrongHelper)


def test_batch_sink_requires_streaming_final_output() -> None:
    class BatchInput(Transform):
        events = input(Event)
        alerts = output(Alert)
        send_alerts = sink(AlertMessage)

        @step(output=alerts)
        def publish(self, event: Event, sink: AlertMessage) -> Alert:
            alert = Alert(id=event.id)
            foreach_batch(alert, sink)
            return alert

    with pytest.raises(StructureCompileError, match="streaming output"):
        _compile(BatchInput)


def test_ambiguous_schema_sinks_require_explicit_selection() -> None:
    class Ambiguous(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        send_alerts = sink(AlertMessage)
        audit_alerts = sink(AlertMessage)

        def publish(self, event: Event, sink: AlertMessage) -> Alert:
            alert = Alert(id=event.id)
            foreach_batch(alert, sink)
            return alert

    with pytest.raises(StructureCompileError, match="matching declarations"):
        _compile(Ambiguous)

    class Selected(Transform):
        events = input(Event, streaming=True)
        alerts = output(Alert)
        send_alerts = sink(AlertMessage)
        audit_alerts = sink(AlertMessage)

        @step(output=alerts, sink=send_alerts)
        def publish(self, event: Event, sink: AlertMessage) -> Alert:
            alert = Alert(id=event.id)
            foreach_batch(alert, sink)
            return alert

    assert cast(PySparkExecutionPlan, _compile(Selected).lowered).sinks[0].name == "send_alerts"
