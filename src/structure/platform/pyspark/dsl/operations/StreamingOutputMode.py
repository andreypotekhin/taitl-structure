from enum import Enum


class StreamingOutputMode(Enum):
    APPEND = "append"
    UPDATE = "update"
    COMPLETE = "complete"
