from enum import Enum


class StreamingSupport(Enum):
    COMPATIBLE = "compatible"
    BATCH_ONLY = "batch_only"
    UNKNOWN = "unknown"
