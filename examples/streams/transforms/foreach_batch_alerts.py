"""Declare a caller-owned foreachBatch handoff for alert delivery."""

from structure import Schema, Transform, input, output, sink, step
from structure.plugin.pyspark import foreach_batch, string


class Event(Schema):
    event_id = string(nullable=False)
    message = string(nullable=False)


class Alert(Schema):
    event_id = string(nullable=False)
    message = string(nullable=False)


class AlertMessage(Schema):
    event_id = string(nullable=False)
    payload = string(nullable=False)


class PublishAlerts(Transform):
    events = input(Event, streaming=True)
    alerts = output(Alert)
    send_alerts = sink(AlertMessage)

    @step(output=alerts)
    def publish(self, event: Event, sink: AlertMessage) -> Alert:
        alert = Alert(event_id=event.event_id, message=event.message)
        foreach_batch(alert, sink)
        return alert


class PrepareAlertBatch(Transform):
    alerts = input(Alert)
    messages = output(AlertMessage)

    def prepare(self, alert: Alert) -> AlertMessage:
        return AlertMessage(event_id=alert.event_id, payload=alert.message)
