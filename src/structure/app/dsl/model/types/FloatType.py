from structure.app.dsl.model.types.ScalarType import ScalarType


class FloatType(ScalarType):

    def __init__(self) -> None:
        super().__init__("float")
