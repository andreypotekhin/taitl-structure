from enum import Enum


class JoinMethod(Enum):
    ONE = "join_one"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    MANY = "join_many"

    def exposes_fields(self) -> bool:
        return self in {JoinMethod.ONE, JoinMethod.MANY}
