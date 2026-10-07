from typing import cast

from structure import *
from structure.core.compiler.api import Compiler
from structure.core.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import *


class Event(Schema):
    id = string(nullable=False)


class CleanEvent(Schema):
    id = string(nullable=False)


class PublishedEvent(Schema):
    id = string(nullable=False)


class CompatibleEvents(StreamingTransform):
    events = input(Event, streaming=True)
    clean = lane(CleanEvent)

    def clean_event(self, event: Event) -> CleanEvent:
        where(event.id.is_not_null())
        return CleanEvent(id=event.id)


class PublishedEvents(CompatibleEvents):
    published = output(PublishedEvent)

    def publish(self, event: CleanEvent) -> PublishedEvent:
        return PublishedEvent(id=event.id)


class IncompatibleEvents(StreamingTransform):
    events = input(Event, streaming=False)
    published = output(PublishedEvent)

    def publish(self, event: Event) -> PublishedEvent:
        checkpoint()
        return PublishedEvent(id=event.id)


def test_streaming_transform_inherits_required_compatibility_without_spark() -> None:
    """I can extend a streaming-compatible transform without repeating a decorator."""
    compilation = Compiler.frontend.compile()(PublishedEvents, materialize_schemas=False)
    plan = cast(TransformPlan, compilation.analysis)

    assert (plan.options or {})["streaming"] is True
    report = Compiler.compileability.streaming()(
        compilation.lowered,
        required=True,
        streaming_contract=True,
    )
    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_streaming_transform_rejects_incompatible_steps_with_batch_inputs() -> None:
    """I get a streaming diagnostic for an incompatible step even with batch input lineage."""
    compilation = Compiler.frontend.compile()(IncompatibleEvents, materialize_schemas=False)
    plan = cast(TransformPlan, compilation.analysis)
    report = Compiler.compileability.streaming()(
        compilation.lowered,
        required=True,
        streaming_contract=True,
    )

    assert plan.inputs[0].streaming is False
    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].code == "STREAM-E0801"
