from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.schemas.schema_api import timestamp


class TimeWindow(Schema):
    """The non-null bounds produced by an event-time ``window(...)`` grouping key."""

    start = timestamp(nullable=False)
    end = timestamp(nullable=False)
