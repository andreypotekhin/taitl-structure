from structure.dsl import FieldDeclaration, Schema
from structure.plugin.pyspark.dsl.types import Timestamp


class TimeWindow(Schema):
    """The non-null bounds produced by an event-time ``window(...)`` grouping key."""

    start = FieldDeclaration(Timestamp(), nullable=False)
    end = FieldDeclaration(Timestamp(), nullable=False)
