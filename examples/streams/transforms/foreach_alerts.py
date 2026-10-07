"""A transform that exposes an opt-in row sink for its final streaming output."""

from importlib import import_module
from typing import TYPE_CHECKING

from examples.streams.foreach_sinks import JsonLinesAlertWriterBehavior
from structure import Schema, Transform, input, output, sink, step

_pyspark_dsl = import_module("structure.plugin.pyspark")
if TYPE_CHECKING:
    from structure.plugin.pyspark.dsl.SinkRole import Sink
else:
    Sink = _pyspark_dsl.Sink
foreach = _pyspark_dsl.foreach
string = _pyspark_dsl.string


class Event(Schema):
    event_id = string(nullable=False)


class Alert(Schema):
    event_id = string(nullable=False)


class AlertWriter(JsonLinesAlertWriterBehavior, Sink[Alert]):
    """Caller constructed writer for the Alert rows in the named handoff."""


class PublishAlerts(Transform):
    events = input(Event, streaming=True)
    alerts = output(Alert)
    publish_alerts = sink(Alert)

    @step(output=alerts)
    def publish(self, event: Event) -> Alert:
        alert = Alert(event_id=event.event_id)
        foreach(alert, self.publish_alerts)
        return alert
