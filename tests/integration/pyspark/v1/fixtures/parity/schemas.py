from structure import *
from structure.plugin.pyspark import *


class RawRow(Schema):
    id = string(nullable=True)


class NormalizedRow(Schema):
    id = string(nullable=True)
    hook_owner = string(nullable=True)


class PublishedRow(Schema):
    id = string(nullable=True)
    hook_owner = string(nullable=True)


class StreamEvent(Schema):
    id = string(nullable=True)
    event_time = timestamp(nullable=True)


class StreamCustomer(Schema):
    id = string(nullable=True)
    value = string(nullable=True)


class StreamEnriched(Schema):
    id = string(nullable=True)
    value = string(nullable=True)
