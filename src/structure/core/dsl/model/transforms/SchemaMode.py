"""Schema compatibility modes for raw target hooks."""

from enum import Enum


class SchemaMode(Enum):
    """How strictly raw hook data must match declared Structure schemas."""

    STRICT = "strict"
    ALLOW_EXTRA_COLUMNS = "allow_extra_columns"
