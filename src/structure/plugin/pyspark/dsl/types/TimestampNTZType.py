from structure.plugin.pyspark.dsl.types.ScalarType import ScalarType


class TimestampNTZType(ScalarType):
    """Spark timestamp without time zone (wall-clock local timestamp)."""

    def __init__(self) -> None:
        super().__init__("timestamp_ntz")
