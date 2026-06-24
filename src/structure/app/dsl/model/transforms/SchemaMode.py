from enum import Enum


class SchemaMode(Enum):
    STRICT = "strict"
    ALLOW_EXTRA_COLUMNS = "allow_extra_columns"
