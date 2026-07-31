from structure.plugin.pyspark.dsl.types.ScalarType import ScalarType


class VariantType(ScalarType):
    """Spark's opaque semi-structured ``VARIANT`` value type."""

    def __init__(self) -> None:
        super().__init__("variant")
