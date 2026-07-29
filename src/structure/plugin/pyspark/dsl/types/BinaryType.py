from structure.plugin.pyspark.dsl.types.ScalarType import ScalarType


class BinaryType(ScalarType):
    def __init__(self) -> None:
        super().__init__("binary")
