from structure.app.dsl.model.types.ScalarType import ScalarType


class BooleanType(ScalarType):

    def __init__(self) -> None:
        super().__init__("boolean")
