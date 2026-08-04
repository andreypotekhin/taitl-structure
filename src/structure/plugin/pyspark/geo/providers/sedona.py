"""Apache Sedona adapter for the common Structure Geometry contract."""


class SedonaProvider:
    def geometry_type(self):
        try:
            from sedona.spark.sql.types import GeometryType  # type: ignore[import-not-found]
        except ImportError:
            from sedona.sql.types import GeometryType  # type: ignore[import-not-found]
        return GeometryType()

    def validate(self, operations: frozenset[str] = frozenset()) -> None:
        self.geometry_type()


def provider() -> SedonaProvider:
    return SedonaProvider()
