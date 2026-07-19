from structure.core.dsl.model.schemas.FieldDeclaration import FieldDeclaration
from structure.core.dsl.model.schemas.Schema import Schema
from structure.platform.pyspark.dsl.types import Timestamp


class TimeWindow(Schema):
    """The non-null bounds produced by an event-time ``window(...)`` grouping key."""

    start = FieldDeclaration(Timestamp(), nullable=False)
    end = FieldDeclaration(Timestamp(), nullable=False)
