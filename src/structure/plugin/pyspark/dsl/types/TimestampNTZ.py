from structure.plugin.pyspark.dsl.types.TimestampNTZType import TimestampNTZType


class TimestampNTZ(TimestampNTZType):
    """Field type for Spark timestamps that do not represent an instant."""
