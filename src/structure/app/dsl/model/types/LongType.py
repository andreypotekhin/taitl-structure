from structure.app.dsl.model.types.ScalarType import ScalarType


class LongType(ScalarType):

    def __init__(self) -> None:
        super().__init__("long")
