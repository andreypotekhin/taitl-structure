from enum import Enum


class JoinStrategy(Enum):
    BROADCAST_HASH = "broadcast_hash"
    SHUFFLE_HASH = "shuffle_hash"
    SORT_MERGE = "sort_merge"

