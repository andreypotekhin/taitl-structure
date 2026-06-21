from structure.app.dsl.logic.model.types.ScalarType import ScalarType


class IntegerType(ScalarType):

    def __init__(self) -> None:
        super().__init__("integer")
