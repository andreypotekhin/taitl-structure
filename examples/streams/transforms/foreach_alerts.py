"""A transform that exposes an opt-in row sink for its final streaming output."""

from importlib import import_module

from examples.streams.foreach_sinks import JsonLinesAlertWriter
from structure import Schema, Transform, input, output, sink, step

_pyspark_dsl = import_module("structure.plugin.pyspark")
foreach = _pyspark_dsl.foreach
string = _pyspark_dsl.string


class Event(Schema):
    event_id = string(nullable=False)


class Alert(Schema):
    event_id = string(nullable=False)


class PublishAlerts(Transform):
    events = input(Event, streaming=True)
    alerts = output(Alert)
    publish_alerts = sink(JsonLinesAlertWriter)

    @step(output=alerts)
    def publish(self, event: Event, sink: JsonLinesAlertWriter) -> Alert:
        alert = Alert(event_id=event.event_id)
        foreach(alert, sink)
        return alert
