from integration.pyspark.v1.fixtures.parity.schemas import (
    NormalizedRow,
    PublishedRow,
    RawRow,
    StreamCustomer,
    StreamEnriched,
    StreamEvent,
)

from structure import *
from structure.plugin.pyspark import *


class NormalizeBase(Transform):
    rows = input(RawRow)
    normalized = lane(NormalizedRow)

    @step(output=normalized)
    def normalize(self, row: RawRow) -> NormalizedRow:
        return NormalizedRow(id=row.id, hook_owner="none")

    @raw(inout=lane(normalized) | lane(normalized))
    def mark(self, *, normalized, spark, ctx):
        from pyspark.sql import functions as F

        return normalized.withColumn("hook_owner", F.lit("parent"))


@transform
class ParentHookPublished(NormalizeBase):
    published = output(PublishedRow)

    def publish(self, row: NormalizedRow) -> PublishedRow:
        return PublishedRow(id=row.id, hook_owner=row.hook_owner)


@transform(streaming_compatible=True)
class WatermarkedLookup(Transform):
    events = input(StreamEvent, streaming=StreamingMode.YES)
    customers = input(StreamCustomer)
    enriched = output(StreamEnriched)

    def enrich(self, event: StreamEvent, customer: StreamCustomer) -> StreamEnriched:
        watermark(event.event_time, delay="10 minutes")
        lookup_join(customer, on=customer.id == event.id, how=Join.LEFT)
        return StreamEnriched(id=event.id, value=customer.value)
