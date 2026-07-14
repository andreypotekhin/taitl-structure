from structure import *


class RawRow(Schema):
    id = field(String(), nullable=True)


class NormalizedRow(Schema):
    id = field(String(), nullable=True)
    hook_owner = field(String(), nullable=True)


class PublishedRow(Schema):
    id = field(String(), nullable=True)
    hook_owner = field(String(), nullable=True)


class StreamEvent(Schema):
    id = field(String(), nullable=True)
    event_time = field(Timestamp(), nullable=True)


class StreamCustomer(Schema):
    id = field(String(), nullable=True, primary_key=True)
    value = field(String(), nullable=True)


class StreamEnriched(Schema):
    id = field(String(), nullable=True)
    value = field(String(), nullable=True)
