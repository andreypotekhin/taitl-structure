from enum import Enum


class OperationCardinality(Enum):
    ROW_PRESERVING = "row_preserving"
    ROW_FILTERING = "row_filtering"
    ROW_MULTIPLYING = "row_multiplying"
    AGGREGATE = "aggregate"
    SELECT_ONE = "select_one"
    UNKNOWN = "unknown"
