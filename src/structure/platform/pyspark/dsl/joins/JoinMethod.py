from enum import Enum


class JoinMethod(Enum):
    LOOKUP = "lookup_join"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    ROWSET = "rowset_join"
    TEMPORAL_ONE = "temporal_one"
    AS_OF_ONE = "as_of_one"

    def exposes_fields(self) -> bool:
        return self in {JoinMethod.LOOKUP, JoinMethod.ROWSET, JoinMethod.TEMPORAL_ONE, JoinMethod.AS_OF_ONE}
