from enum import Enum


class JoinStrategy(Enum):
    BROADCAST_HASH = "broadcast_hash"
    SHUFFLE_HASH = "shuffle_hash"
    SORT_MERGE = "sort_merge"
    SHUFFLE_REPLICATE_NL = "shuffle_replicate_nl"

    def hint(self) -> str:
        return {
            JoinStrategy.BROADCAST_HASH: "broadcast",
            JoinStrategy.SHUFFLE_HASH: "shuffle_hash",
            JoinStrategy.SORT_MERGE: "merge",
            JoinStrategy.SHUFFLE_REPLICATE_NL: "shuffle_replicate_nl",
        }[self]
