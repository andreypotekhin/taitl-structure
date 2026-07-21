from structure.plugin.pyspark.dsl.types.ScalarType import ScalarType


class DateType(ScalarType):
    def __init__(self) -> None:
        super().__init__("date")
