from structure import *


class RawRow(Schema):
    id = field.string(nullable=True)


class NormalizedRow(Schema):
    id = field.string(nullable=True)
    hook_owner = field.string(nullable=True)


class PublishedRow(Schema):
    id = field.string(nullable=True)
    hook_owner = field.string(nullable=True)


class StreamEvent(Schema):
    id = field.string(nullable=True)
    event_time = field.timestamp(nullable=True)


class StreamCustomer(Schema):
    id = field.string(nullable=True)
    value = field.string(nullable=True)


class StreamEnriched(Schema):
    id = field.string(nullable=True)
    value = field.string(nullable=True)
