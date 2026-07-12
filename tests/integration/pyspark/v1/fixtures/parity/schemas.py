from structure import String, Structure, Timestamp, field


class RawRow(Structure):
    id = field(String(), nullable=True)


class NormalizedRow(Structure):
    id = field(String(), nullable=True)
    hook_owner = field(String(), nullable=True)


class PublishedRow(Structure):
    id = field(String(), nullable=True)
    hook_owner = field(String(), nullable=True)


class StreamEvent(Structure):
    id = field(String(), nullable=True)
    event_time = field(Timestamp(), nullable=True)


class StreamCustomer(Structure):
    id = field(String(), nullable=True, primary_key=True)
    value = field(String(), nullable=True)


class StreamEnriched(Structure):
    id = field(String(), nullable=True)
    value = field(String(), nullable=True)
