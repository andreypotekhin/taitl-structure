from enum import Enum


class JoinMethod(Enum):
    ONE = "join_one"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    MANY = "join_many"
    ROWSET = "join_rowset"
    TEMPORAL_ONE = "temporal_one"
    AS_OF_ONE = "as_of_one"

    def exposes_fields(self) -> bool:
        return self in {
            JoinMethod.ONE,
            JoinMethod.MANY,
            JoinMethod.ROWSET,
            JoinMethod.TEMPORAL_ONE,
            JoinMethod.AS_OF_ONE,
        }
